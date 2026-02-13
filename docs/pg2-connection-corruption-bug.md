# pg2 Connection Corruption: Null ParameterDescription and Stream Misalignment

## Summary

pg2 0.1.41 (and all versions through current master) has a connection corruption bug where `readTypesProcess()` can leave a connection's input stream misaligned. The corrupted connection is returned to the pool without validation, causing cascading failures on all subsequent operations: `NullPointerException` in `sendBind` and `PGError("Wrong parameters count")`.

## Symptoms

1. **NPE in sendBind**:
   ```
   java.lang.NullPointerException: Cannot invoke "org.pg.msg.server.ParameterDescription.oids()"
   because the return value of "org.pg.PreparedStatement.parameterDescription()" is null
       at org.pg.Connection.sendBind(Connection.java:735)
       at org.pg.Connection.execute(Connection.java:873)
   ```

2. **Parameter count mismatches** — queries receive ParameterDescriptions from *different* queries:
   ```
   org.pg.error.PGError: Wrong parameters count: 4 (must be 2)
   org.pg.error.PGError: Wrong parameters count: 1 (must be 4)
   org.pg.error.PGError: Wrong parameters count: 2 (must be 1)
   ```

3. **Cascading permanent failure** — once a connection is corrupted, every subsequent operation on it fails. Multiple pool connections become corrupted over time.

## Root Cause Analysis

### 1. `readTypesProcess()` has an incomplete message handler

[`Connection.java:226-272`](https://github.com/igrishaev/pg2/blob/master/pg-core/src/java/org/pg/Connection.java#L226-L272)

```java
private List<PGType> readTypesProcess(final String query) {
    sendQuery(query);
    flush();
    // ...
    while (true) {
        msg = readMessage(false);
        if (msg instanceof CopyData copyData) {
            // ... handle binary row
        } else if (msg instanceof CopyOutResponse
                || msg instanceof CopyDone
                || msg instanceof CommandComplete) {
            // skip
        } else if (msg instanceof ReadyForQuery) {
            break;
        } else if (msg instanceof ErrorResponse e) {
            errorResponse = e;
        } else {
            throw new PGError("Unexpected message in readTypes: %s", msg);  // <-- HERE
        }
    }
    // ...
}
```

This method does **NOT** handle `NoticeResponse` or `ParameterStatus`, both of which PostgreSQL can send during query processing. The main `handleMessage()` method at line 970 handles both correctly, but `readTypesProcess` has its own independent message loop.

If a `NoticeResponse` or `ParameterStatus` arrives during the type resolution COPY query, `readTypesProcess` throws `PGError`. The exception exits the while loop **without consuming the remaining messages** (including `ReadyForQuery`). The connection's input stream now has unread data from the interrupted type resolution query.

### 2. VOID type (OID 2278) is not pre-registered, triggering `readTypesProcess`

[`Processors.java` static initializer](https://github.com/igrishaev/pg2/blob/master/pg-core/src/java/org/pg/processor/Processors.java)

The `VOID` type (OID 2278) has no processor registered in the default `oidMap`. Common queries that return VOID trigger type resolution:

```sql
SELECT pg_notify($1, $2)    -- Connection.notify() uses this
```

When `prepareUnlocked()` processes this query:

```java
// Connection.java:706-728
private PreparedStatement prepareUnlocked(String sql, ExecuteParams executeParams) {
    // ... sends Parse, DescribeStatement, Flush, Sync ...
    final Result res = interact(sql);                              // consumes all responses ✓
    final ParameterDescription pd = res.getParameterDescription();
    final RowDescription rd = res.getRowDescription();
    final int[] oidsRD = rd == null ? null : rd.oids();            // RowDescription has VOID OID
    final int[] oidsPD = pd == null ? null : pd.oids();
    final Set<Integer> oidsUnknown = unsupportedOids(oidsRD, oidsPD);  // {2278} → not empty!
    setTypesByOids(oidsUnknown);                                   // triggers readTypesProcess()
    return new PreparedStatement(parse, pd, rd);
}
```

The `RowDescription` for `SELECT pg_notify($1, $2)` contains VOID (OID 2278). Since VOID is not in `Processors.isKnownOid()`, `unsupportedOids()` returns `{2278}`, triggering `setTypesByOids()` → `readTypesOids()` → `readTypesProcess()`.

### 3. Pool returns corrupted connections without validation

[`Pool.java:199-267`](https://github.com/igrishaev/pg2/blob/master/pg-core/src/java/org/pg/Pool.java#L199-L267)

```java
public void returnConnection(Connection conn, boolean forceClose) {
    if (conn.isTxError()) { rollback(); closeConnection(conn); return; }
    if (conn.isTransaction()) { rollback(); addFree(conn); return; }
    if (conn.isClosed()) { removeUsed(conn); return; }
    // Default: return to pool with NO stream validation
    removeUsed(conn);
    addFree(conn);  // corrupted connection goes back to free queue
}
```

There is no check for:
- Unread data in the input stream (`InputStream.available() > 0`)
- Protocol state consistency
- Connection health (no ping/validation query)

The Clojure `with-connection` macro unconditionally returns the connection:

```clojure
;; pg/core.clj
(defmacro with-connection [[bind src] & body]
  `(let [~bind (src/-borrow-connection src#)]
     (try ~@body
       (finally (src/-return-connection src# ~bind)))))
```

For a Pool, `-return-connection` calls `Pool.returnConnection(conn)` with `forceClose=false`.

### 4. Null ParameterDescription creates NPE in sendBind

[`Connection.java:730-741`](https://github.com/igrishaev/pg2/blob/master/pg-core/src/java/org/pg/Connection.java#L730-L741)

```java
private void sendBind(String portal, PreparedStatement stmt, ExecuteParams executeParams) {
    final List<Object> params = executeParams.params();
    final int[] OIDs = stmt.parameterDescription().oids();  // NPE if parameterDescription is null
    final int size = params.size();
    if (size != OIDs.length) {
        throw new PGError("Wrong parameters count: %d (must be %d)", size, OIDs.length);
    }
    // ...
}
```

`PreparedStatement` is a Java record that accepts null for `parameterDescription`:

```java
public record PreparedStatement(
    Parse parse,
    ParameterDescription parameterDescription,  // can be null
    RowDescription rowDescription
)
```

When a corrupted connection's `interact()` reads stale bytes from a previous operation's response, it may never receive a `ParameterDescription` message, leaving the field null.

## Corruption Cascade Mechanism

```
1. Thread borrows Connection C from Pool
2. pg/notify → execute("select pg_notify($1, $2)") → prepareUnlocked()
3. interact() succeeds, RowDescription has VOID OID 2278
4. unsupportedOids({2278}) → non-empty → setTypesByOids() → readTypesProcess()
5. readTypesProcess sends COPY query, starts reading responses
6. Server sends NoticeResponse (or ParameterStatus) during COPY
7. readTypesProcess throws PGError("Unexpected message in readTypes")
8. Unread COPY response data remains in Connection's InputStream
9. Exception propagates: readTypesProcess → setTypesByOids → prepareUnlocked → execute
10. with-connection finally block → Pool.returnConnection(C, false)
11. Pool checks: isTxError? NO. isTransaction? NO. isClosed? NO.
12. Pool returns C to free queue — WITH CORRUPTED STREAM

Next operation on C:
13. Thread borrows C from pool
14. execute() → prepareUnlocked() → sends Parse+Describe+Flush+Sync
15. interact() reads STALE ReadyForQuery from step 5's unfinished COPY query
16. interact() returns immediately with empty Result (no ParameterDescription)
17. PreparedStatement created with null parameterDescription
18. sendBind() → NPE on stmt.parameterDescription().oids()
19. C returned to pool again, now Parse+Describe responses from step 14 are unread
20. EVERY subsequent operation on C fails with wrong ParameterDescription or NPE
```

## Evidence from Integration Tests

Test output shows the corruption pattern clearly — parameter counts from different queries leak across operations:

| Error | Actual params | PD says | Likely source of stale PD |
|-------|--------------|---------|--------------------------|
| `Wrong parameters count: 4 (must be 2)` | update-session (4) | pg_notify (2) |
| `Wrong parameters count: 1 (must be 4)` | fetch-session (1) | update-session (4) |
| `Wrong parameters count: 2 (must be 1)` | pg_notify (2) | fetch-session (1) |
| NPE | any | null | No ParameterDescription received |

31 errors and 25 failures across 421 tests, all starting from a single corruption event ~5 minutes into the test run. The errors cascade as multiple pool connections become corrupted.

## Proposed Fixes

### Fix 1: Handle NoticeResponse and ParameterStatus in readTypesProcess (Critical)

```java
private List<PGType> readTypesProcess(final String query) {
    sendQuery(query);
    flush();
    // ...
    while (true) {
        msg = readMessage(false);
        if (msg instanceof CopyData copyData) {
            // ... existing handling
        } else if (msg instanceof CopyOutResponse
                || msg instanceof CopyDone
                || msg instanceof CommandComplete) {
            // skip
+       } else if (msg instanceof NoticeResponse) {
+           // PostgreSQL can send notices during any query processing
+           handleNoticeResponse((NoticeResponse) msg);
+       } else if (msg instanceof ParameterStatus) {
+           // PostgreSQL can send parameter status during any query processing
+           handleParameterStatus((ParameterStatus) msg);
        } else if (msg instanceof ReadyForQuery) {
            break;
        } else if (msg instanceof ErrorResponse e) {
            errorResponse = e;
        } else {
            throw new PGError("Unexpected message in readTypes: %s", msg);
        }
    }
    // ...
}
```

### Fix 2: Pre-register VOID type processor (Preventive)

Add VOID to the default processors to avoid triggering readTypesProcess for common operations like `pg_notify`:

```java
// In Processors.java static initializer
oidMap.set(OID.VOID, new Void());  // Void processor that decodes to nil/null
```

### Fix 3: Validate connection on pool return (Safety net)

```java
public void returnConnection(Connection conn, boolean forceClose) {
    // ... existing checks ...

    // Check for protocol desync: if there's unread data, the connection is corrupted
    try {
        if (conn.getInputStream().available() > 0) {
            logger.log(Level.WARNING, "Connection {0} has unread data, closing", conn.getId());
            closeConnection(conn);
            removeUsed(conn);
            return;
        }
    } catch (IOException e) {
        closeConnection(conn);
        removeUsed(conn);
        return;
    }

    // Default: return to pool
    removeUsed(conn);
    addFree(conn);
}
```

### Fix 4: Null-check in sendBind (Defensive)

```java
private void sendBind(String portal, PreparedStatement stmt, ExecuteParams executeParams) {
    final ParameterDescription pd = stmt.parameterDescription();
    if (pd == null) {
        throw new PGError(
            "ParameterDescription is null for statement '%s'. " +
            "This indicates a corrupted connection (protocol desync).",
            stmt.parse().sql()
        );
    }
    final int[] OIDs = pd.oids();
    // ...
}
```

## Environment

- pg2 version: 0.1.41 (bug exists in all versions through current master 0.1.43-SNAPSHOT)
- PostgreSQL: 17
- Java: 21+
- Platform: Linux (Ubuntu)
- Pool config: `{:binary-encode? true, :binary-decode? true, :ps-cache? false}`

## Reproduction

See test in `src/test/com/fulcrologic/statecharts/persistence/pg/pg2_corruption_repro_test.clj`.

The bug manifests most reliably under concurrent load with mixed query types (different parameter counts) and `pg_notify` calls, which trigger VOID type resolution via `readTypesProcess`.
