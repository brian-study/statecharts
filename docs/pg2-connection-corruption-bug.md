# pg2 Pool Connection Corruption: Stale Async Messages Cause Null ParameterDescription

## Summary

pg2 0.1.44 connection pool lacks validation on borrow and return. PostgreSQL async messages (`ParameterStatus`, `NoticeResponse`, `NotificationResponse`) that arrive while a connection sits idle in the pool accumulate in the TCP buffer, causing protocol desync on the next operation. This results in `NullPointerException` in `sendBind` and cascading permanent failure across pool connections.

## Symptoms

```
java.lang.NullPointerException: Cannot invoke "org.pg.msg.server.ParameterDescription.oids()"
because the return value of "org.pg.PreparedStatement.parameterDescription()" is null
    at org.pg.Connection.sendBind(Connection.java:735)
    at org.pg.Connection.execute(Connection.java:873)
```

Also manifests as wrong parameter counts when stale `ParameterDescription` from a different query leaks across operations:
```
org.pg.error.PGError: Wrong parameters count: 4 (must be 2)
```

## Root Cause

PostgreSQL can send async messages at **any time**, including while a connection sits idle in the pool:
- `ParameterStatus` — when a session parameter changes (e.g., timezone)
- `NoticeResponse` — advisory messages
- `NotificationResponse` — from `LISTEN/NOTIFY`

The pg2 pool (`Pool.java`) has no mechanism to handle these:
- **On return**: no check for unread data in the input stream
- **On borrow**: no drain of stale async messages

### Corruption cascade

```
1. Thread completes operation, interact() consumes through ReadyForQuery
2. returnConnection: no stream validation → connection added to free queue
3. PostgreSQL sends ParameterStatus → bytes sit in TCP buffer
4. Thread borrows connection with stale bytes
5. prepareUnlocked() sends Parse+Describe+Flush+Sync
6. interact() reads stale bytes:
   - If async message (ParameterStatus etc.): handled OK, continues normally
   - If stale ReadyForQuery: isEnough() returns true → loop breaks early
     → PreparedStatement created with null parameterDescription
     → sendBind() → NPE
7. Connection returned to pool with Parse+Describe responses still unread
8. EVERY subsequent operation on this connection fails
```

### Trigger pattern

The bug manifests most readily with rapid pool borrow/return cycles. Our statechart persistence layer does:
```
save-working-memory! → pg/with-connection → upsert-session! → return connection
pg_notify            → pg/with-connection → execute          → return connection
```

With a small pool (2-4 connections), the same connection is borrowed and returned many times per second, maximizing exposure to the TCP race window where async messages arrive between return and borrow.

## Evidence

| pg2 version | Test suite | pg2 errors |
|-------------|-----------|------------|
| 0.1.44 (unpatched) | 3357 tests | **20** |
| 0.1.44 + pool drain-on-borrow + return check | 3358 tests | **0** |

All 20 errors in unpatched 0.1.44 were `Protocol desync: parameterDescription is null in sendBind` during `INSERT INTO statechart_sessions` — the operation immediately following a `pg_notify` call on the same pool.

## Fix

Three changes to pg2, all in `Pool.java` and `Connection.java`:

### 1. `Connection.drainAsyncMessages()` — drain stale messages

```java
public void drainAsyncMessages() {
    try (final TryLock ignored = lock.get()) {
        while (IOTool.available(inStream) > 0) {
            final IServerMessage msg = readMessage(false);
            if (msg instanceof NoticeResponse nr) {
                handleNoticeResponse(nr);
            } else if (msg instanceof ParameterStatus ps) {
                handleParameterStatus(ps);
            } else if (msg instanceof NotificationResponse notif) {
                handleNotificationResponse(notif);
            } else {
                throw new PGError(
                    "Protocol desync: unexpected message %s found in stream " +
                    "during drain. Connection is corrupted and will be discarded.",
                    msg
                );
            }
        }
    }
}
```

Handles the three async message types PostgreSQL can send at any time. Throws on anything else (true stream corruption — connection must be discarded).

### 2. `Pool.validateOnBorrow()` — validate before handing to caller

```java
private boolean validateOnBorrow(final Connection conn) {
    try {
        conn.drainAsyncMessages();
        return true;
    } catch (Exception e) {
        logger.log(System.Logger.Level.WARNING,
            "Connection {0} failed validation on borrow (pool {1}): {2}",
            conn.getId(), id, e.getMessage());
        closeConnection(conn);
        return false;
    }
}
```

Called in `borrowConnection()` for every connection polled from the free queue. If validation fails, the connection is discarded and the next one is tried. Newly spawned connections skip validation (no stale data possible).

Applied at all three borrow sites in `borrowConnection()`:
- Free queue poll loop
- Timed poll (waiting for a connection)

### 3. `Pool.returnConnection` hasUnreadData check — defense in depth

```java
if (conn.hasUnreadData()) {
    logger.log(System.Logger.Level.WARNING,
        "Connection {0} has unread data in input stream on return, closing (pool {1})",
        conn.getId(), id);
    closeConnection(conn);
    removeUsed(conn);
    return;
}
```

Added before `addFree(conn)`. Catches obvious corruption immediately on return. Has a TCP race window (bytes may arrive after the check), which is why drain-on-borrow is the primary defense.

### 4. `Connection.sendBind` null-check — clear error instead of NPE

```java
final ParameterDescription pd = stmt.parameterDescription();
if (pd == null) {
    throw new PGError(
        "Protocol desync: parameterDescription is null in sendBind. " +
        "The server did not send a ParameterDescription during prepare. " +
        "This may indicate a corrupted connection or that an earlier query " +
        "left unread data in the input stream. SQL: %s, params: %s",
        stmt.parse().query(), params
    );
}
```

Diagnostic improvement only — with drain-on-borrow this path should never be reached.

## Environment

- pg2 base version: 0.1.44
- PostgreSQL: 13, 16, 17
- Java: 21+
- Pool config: `{:binary-encode? true, :binary-decode? true}`

## Reproduction

See test in `pg-core/test/pg/sendBind_null_check_test.clj`.

The bug manifests under concurrent load with mixed query types and `pg_notify` calls on a small pool. The statechart session persistence pattern (rapid sequential borrow/return with pg_notify between saves) is a reliable trigger.
