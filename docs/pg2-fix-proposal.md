# pg2 Fix Proposal: Connection Corruption via readTypesProcess

See [pg2-connection-corruption-bug.md](pg2-connection-corruption-bug.md) for the full bug report.

## Overview

Four changes, in priority order. Fix 1 alone resolves the root cause. Fixes 2-4 add defense in depth.

---

## Fix 1: Handle async messages in readTypesProcess (Root Cause)

**File:** `pg-core/src/java/org/pg/Connection.java` (lines 226-272)

**Problem:** `readTypesProcess()` has its own message loop that only handles COPY-related messages. PostgreSQL can send `NoticeResponse`, `ParameterStatus`, and `NotificationResponse` at any time during query processing (per the [PostgreSQL protocol docs](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-ASYNC)). When any of these arrive during the COPY flow, `readTypesProcess` throws `PGError("Unexpected message in readTypes")`, which exits the loop **without consuming `ReadyForQuery`**, leaving the connection's input stream misaligned.

**Fix:** Handle async messages the same way `handleMessage()` (line 970) does:

```java
private List<PGType> readTypesProcess(final String query) {
    sendQuery(query);
    flush();
    IServerMessage msg;
    PGType pgType;
    ByteBuffer bb;
    boolean headerSeen = false;
    final List<PGType> result = new ArrayList<>();
    ErrorResponse errorResponse = null;
    while (true) {
        msg = readMessage(false);
        if (Debug.isON) {
            Debug.debug(" -> %s", msg);
        }
        if (msg instanceof CopyData copyData) {
            bb = copyData.buf();
            if (!headerSeen) {
                BBTool.skip(bb, Copy.COPY_BIN_HEADER.length);
                headerSeen = true;
            }
            if (Copy.isTerminator(bb)) {
                continue;
            }
            pgType = PGType.fromCopyBuffer(bb);
            if (Debug.isON) {
                Debug.debug(" -> %s\r\n", pgType);
            }
            result.add(pgType);
        } else if (msg instanceof CopyOutResponse
                || msg instanceof CopyDone
                || msg instanceof CommandComplete) {
            if (Debug.isON) {
                Debug.debug(" -> skipping message: %s", msg);
            }
        // --- BEGIN NEW CODE ---
        } else if (msg instanceof NoticeResponse nr) {
            // PostgreSQL can send NoticeResponse asynchronously during any query.
            // Delegate to the same handler used by handleMessage().
            handleNoticeResponse(nr);
        } else if (msg instanceof ParameterStatus ps) {
            // PostgreSQL can send ParameterStatus asynchronously (e.g., SET, reload).
            handleParameterStatus(ps);
        } else if (msg instanceof NotificationResponse notif) {
            // PostgreSQL can send NotificationResponse to LISTENing connections
            // at any time, including during COPY protocol processing.
            // We need a Result to call handleNotificationResponse, but we don't
            // have one in this context. Store the notification directly.
            final boolean isSelf = notif.pid() == pid;
            final Object obj = notif.toClojure().assoc(KW.self_QMARK, isSelf);
            final IFn handler = config.fnNotification();
            if (handler == null) {
                notifications.add(obj);
            } else {
                handlerCall(handler, obj);
            }
        // --- END NEW CODE ---
        } else if (msg instanceof ReadyForQuery) {
            break;
        } else if (msg instanceof ErrorResponse e) {
            errorResponse = e;
        } else {
            throw new PGError("Unexpected message in readTypes: %s", msg);
        }
    }
    if (errorResponse != null) {
        throw new PGErrorResponse(errorResponse);
    }
    return result;
}
```

**Why this works:** The three async message types (`NoticeResponse`, `ParameterStatus`, `NotificationResponse`) are the only messages PostgreSQL sends outside the normal query flow. By handling them in `readTypesProcess` the same way `handleMessage()` does, the COPY loop can continue to `ReadyForQuery` without throwing, keeping the connection's stream aligned.

**Note on NotificationResponse:** The existing `handleNotificationResponse(msg, res)` requires a `Result` parameter. In `readTypesProcess` there's no Result object. The simplest approach is to inline the notification handling (as shown above). Alternatively, you could extract a `handleNotificationResponse(NotificationResponse msg)` overload that doesn't need a Result.

---

## Fix 2: Pre-register VOID type processor (Preventive)

**File:** `pg-core/src/java/org/pg/processor/Processors.java` (static initializer, line 64)

**Problem:** VOID (OID 2278) is not in the default `oidMap`. Any query returning VOID (e.g., `SELECT pg_notify($1, $2)`) triggers `unsupportedOids()` → `setTypesByOids()` → `readTypesProcess()` on first use per connection. This is unnecessary since VOID values don't carry data.

**Fix:** Add a Void processor and register it:

New file `pg-core/src/java/org/pg/processor/Void.java`:
```java
package org.pg.processor;

import org.pg.codec.CodecParams;
import java.nio.ByteBuffer;

/**
 * Processor for the VOID type (OID 2278).
 * PostgreSQL returns VOID for functions with no meaningful return value
 * (e.g., pg_notify). The value is always empty/null.
 */
public class Void extends AProcessor {

    @Override
    public Object decodeBin(final ByteBuffer bb, final CodecParams codecParams) {
        return null;
    }

    @Override
    public Object decodeTxt(final String text, final CodecParams codecParams) {
        return null;
    }

    @Override
    public ByteBuffer encodeBin(final Object x, final CodecParams codecParams) {
        return ByteBuffer.allocate(0);
    }

    @Override
    public String encodeTxt(final Object x, final CodecParams codecParams) {
        return "";
    }
}
```

In `Processors.java`, add to the static initializer:
```java
// misc (existing)
oidMap.set(OID.UUID, new Uuid());
// ... existing entries ...
oidMap.set(OID.BOOL, new Bool());
oidMap.set(OID.BIT, new Bit());
// void - returned by functions like pg_notify()
oidMap.set(OID.VOID, new Void());
```

**Why this helps:** With VOID pre-registered, `isKnownOid(2278)` returns true, `unsupportedOids()` returns an empty set, and `readTypesProcess()` is never called for `pg_notify`. This eliminates the most common trigger for the bug (though Fix 1 is still needed since other unregistered OIDs could trigger the same path).

---

## Fix 3: Null-check in sendBind (Defensive)

**File:** `pg-core/src/java/org/pg/Connection.java` (line 730-735)

**Problem:** If `prepareUnlocked()` returns a `PreparedStatement` with null `parameterDescription` (which can happen when the stream is misaligned and `interact()` never receives a `ParameterDescription` message), `sendBind` throws `NullPointerException` at `stmt.parameterDescription().oids()`. The NPE has no diagnostic information and is confusing.

**Fix:** Add a null check with a clear error message:

```java
private void sendBind (final String portal,
                       final PreparedStatement stmt,
                       final ExecuteParams executeParams
) {
    final List<Object> params = executeParams.params();
    final ParameterDescription pd = stmt.parameterDescription();
    if (pd == null) {
        throw new PGError(
            "ParameterDescription is null for statement '%s' (SQL: %s). " +
            "This indicates a protocol desync — the connection may be corrupted.",
            stmt.parse().statement(),
            stmt.parse().sql()
        );
    }
    final int[] OIDs = pd.oids();
    final int size = params.size();
    // ... rest unchanged
```

**Why this helps:** Converts an opaque NPE into a clear `PGError` with diagnostic context. Doesn't fix the root cause but makes debugging much easier.

---

## Fix 4: Validate connection on pool return (Safety Net)

**File:** `pg-core/src/java/org/pg/Pool.java` (lines 262-266)

**Problem:** `returnConnection()` checks `isTxError()`, `isTransaction()`, and `isClosed()`, but does not check for protocol-level corruption. A connection with unread bytes in its input stream is returned to the free queue and poisons the next borrower.

**Fix:** Before returning to the free queue, check for pending data:

```java
// else (final block before returning to pool, line 262)

// Check for protocol desync: unread data means the connection is corrupted
if (conn.hasUnreadData()) {
    logger.log(System.Logger.Level.WARNING,
        "Connection {0} has unread data in input stream, closing (pool {1})",
        conn.getId(), id);
    closeConnection(conn);
    try (TryLock ignored = lock.get()) {
        removeUsed(conn);
    }
    return;
}

try (TryLock ignored = lock.get()) {
    removeUsed(conn);
    addFree(conn);
}
```

This requires adding a `hasUnreadData()` method to `Connection.java`:

```java
/**
 * Check if the connection's input stream has unread data.
 * Unread data after a complete operation indicates protocol desync.
 * Used by Pool to detect corrupted connections before returning them.
 */
public boolean hasUnreadData() {
    try {
        return inputStream.available() > 0;
    } catch (IOException e) {
        // If we can't check, assume corrupted
        return true;
    }
}
```

Where `inputStream` is the `InputStream` field on the Connection (the actual field name may be `inStream` — adjust accordingly).

**Why this helps:** Even if Fix 1 is incomplete or another code path leaves unread data, the pool won't recycle corrupted connections. This is a safety net that prevents cascading failures.

**Caveat:** `InputStream.available()` is not guaranteed to return nonzero even when data is available (it's documented as "an estimate"). For TCP sockets, `SocketInputStream.available()` does return the actual buffered byte count on Linux/macOS, so this is reliable in practice. An alternative would be a non-blocking read with `SO_TIMEOUT=0`, but `available()` is simpler and sufficient.

---

## Summary

| Fix | Type | Effort | Impact |
|-----|------|--------|--------|
| 1. Handle async messages in readTypesProcess | Root cause | Small | Eliminates the corruption mechanism |
| 2. Pre-register VOID processor | Preventive | Small | Eliminates the most common trigger |
| 3. Null-check in sendBind | Defensive | Trivial | Better error messages |
| 4. Validate pool returns | Safety net | Small | Prevents cascading failures from any corruption source |

Fix 1 alone is sufficient. Fixes 2-4 add defense in depth. Fix 2 is the quickest win since it avoids triggering `readTypesProcess` for the most common case (`pg_notify`).
