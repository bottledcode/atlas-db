# Atlas Socket Protocol

This document describes the line-oriented control protocol implemented in `atlas/socket`. It is the canonical reference
for client authors and operators building tooling against Atlas’ interactive socket.

## 1. Scope and audience

- **Audience**: client programmers, SREs operating Atlas clusters, and contributors maintaining the socket layer.
- **Out of scope**: the gRPC APIs exposed through the Caddy module (bootstrap/consensus) and the HTTP surface of Caddy
  itself.
- **Versioning**: protocol frames advertise version `1.0`. All information here reflects the behaviour of
  `atlas/socket` at commit `6ee5319` and later.

## 2. Transport assumptions

- **Endpoint**: a Unix domain socket whose location is configured through `atlas/options.Options.SocketPath`
  (`atlas.sock` by default). The Caddy module ensures the containing directory exists and sets appropriate permissions.
- **Concurrency**: every accepted connection is handled on its own goroutine; reads and writes are multiplexed using a
  scanner goroutine plus a guarded writer with a `sync.Mutex`.
- **Timeouts**: write operations inherit a five minute deadline (`Socket.timeout`). Read deadlines are not set so long
  running operations (e.g. large scans) do not fail spuriously.
- **Maximum frame size**: individual command lines are limited to 128 MiB (`maxScannerLength`). Exceeding this limit
  terminates the connection with `ERROR FATAL command exceeded maximum length`.

## 3. Connection lifecycle

Atlas connections progress through the following states:

| State         | Trigger                                            | Server behaviour                                                              |
|---------------|----------------------------------------------------|-------------------------------------------------------------------------------|
| `Greeting`    | TCP/Unix accept                                    | Immediately write `WELCOME 1.0 Chronalys/1.0\r\n`.                            |
| `Handshake`   | Client responds                                    | Validate first frame equals `HELLO 1.0 <token>` (token may be any printable). |
| `Ready`       | Handshake accepted                                 | Emit `READY\r\n` and start streaming commands to the executor.                |
| `Operational` | Commands flowing                                   | Process synchronously or asynchronously depending on request ID tagging.      |
| `Closing`     | Context cancelled, fatal error, or socket teardown | Flush pending writes, close connection, release resources.                    |

Failure to send a correctly formatted `HELLO` leads to `ERROR FATAL invalid handshake` and an immediate close. Atlas
ignores the client identifier today but reserves it for future feature negotiation.

## 4. Frame grammar

All requests and textual responses are ASCII frames terminated by carriage-return/newline. Binary payloads are the only
exception (see §7).

```
frame        := tagged-frame | command-line
command-line := token *(SP token) CR LF
Tagged       := "[ID:" 1*VCHAR "]"                ; request identifier (no closing bracket)
tagged-frame := Tagged SP command-line
```

- Tokens are split by ASCII whitespace. The server stores an uppercase copy for dispatch while retaining the raw tokens
  for value parsing.
- Multiple spaces are preserved in the raw view so commands such as `KEY PUT foo.bar   padded value` retain the intended
  value payload.
- Empty lines are ignored.

## 5. Request identifiers and concurrency

Any command prefixed with `[ID:<token>]` is executed asynchronously:

1. `CommandFromString` extracts the ID and removes it from the raw command passed to the command dispatcher.
2. The dispatcher spins up a goroutine that calls `Socket.executeCommandAsync` with the captured ID and the principal in
   effect at submission time.
3. Responses emitted from the command include the same prefix:
    - Data frames become `[ID:<token>] <payload>`.
    - Binary headers become `[ID:<token>] BLOB <n>`.
    - Terminal acknowledgements become `[ID:<token>] OK`.
4. Errors use `ERROR WARN`/`ERROR FATAL` but still carry the prefix.

Commands without an ID execute synchronously and block the read loop until they finish.

## 6. Error semantics

Errors propagate through `socket.formatCommandError` which maps Go/ gRPC errors to protocol levels:

| Level   | Meaning                                      | Connection state                 |
|---------|----------------------------------------------|----------------------------------|
| `WARN`  | Command failed but connection remains usable | Continue reading next command    |
| `INFO`  | Reserved for streaming hints (unused today)  | Continue                         |
| `FATAL` | Non-recoverable protocol violation           | Connection is closed immediately |

The formatter sanitises control characters to prevent frame injection. Permission failures include the active
principal (`permission denied for principal 'alice': ...`) when available.

## 7. Binary transfers

Binary support exists exclusively for `KEY BLOB` operations.

- **Upload (`KEY BLOB SET`)**
    1. Client sends `KEY BLOB SET <key> <length>\r\n`.
    2. Scanner reads the declared number of bytes directly from the socket (`io.ReadFull`). No trailing newline is
       expected; the next byte after the payload is treated as the first byte of the next frame.
    3. Command executes synchronously or asynchronously (depending on request ID) and replies just with `OK`.
- **Download (`KEY BLOB GET`)**
    1. Command execution returns raw bytes from the key/value store.
    2. The socket layer writes `BLOB <length>\r\n` (with the optional `[ID:...]` prefix) and streams the payload
       verbatim.
    3. A terminal `OK\r\n` frame acknowledges completion. `nil` payloads produce `EMPTY\r\nOK\r\n` instead.

Binary size is bounded by the same 128 MiB guard used for command lines.

## 8. Principals and metadata propagation

Atlas propagates caller identity through per-connection principals:

- `PRINCIPAL ASSUME <name>` validates the supplied name (trimmed, 1–256 runes, no control characters) and stores it on
  the `Socket`.
- The current value is injected into gRPC outgoing metadata as key `atlas-principal` for all subsequent commands.
- `PRINCIPAL WHOAMI` reports the stored principal, defaulting to `(none)` when unset.
- Principals interact with ACL enforcement and appear in audit logs emitted by key commands.

## 9. Flow-control considerations

- Atlas does not currently implement server-initiated flow control. Clients should not pipeline multiple `KEY BLOB SET`
  bodies; wait for the `OK` frame before streaming the next payload.
- For asynchronous commands, clients must keep track of outstanding IDs and handle out-of-order responses. The server
  does
  not limit concurrency beyond resource limits in the command implementations.
- Long-running commands (e.g. `SCAN` across the cluster) stream their result lines before the terminating `OK`. Use
  `[ID:...]` if you want other commands to proceed while a scan executes.

## 10. Connection termination

A connection closes when:

- The server writes `ERROR FATAL ...` (e.g. handshake failure, malformed command).
- The underlying context is cancelled (service shutdown) or the client disconnects.
- An I/O error occurs when writing responses.

Clients should treat EOF as an implicit failure of the in-flight command and retry as appropriate.

## 11. Example transcript

```
# 1. Handshake
S: WELCOME 1.0 Chronalys/1.0\r\n
C: HELLO 1.0 cli=atlasctl\r\n
S: READY\r\n

# 2. Set principal and seed text value
C: PRINCIPAL ASSUME analytics\r\n
S: OK\r\n
C: KEY PUT demo.row value with spaces\r\n
S: OK\r\n

# 3. Concurrent blob downloads using request IDs
C: [ID:a] KEY BLOB GET log.archive\r\n
C: [ID:b] KEY BLOB GET asset.raw\r\n
S: [ID:b] BLOB 2048\r\n<2048 bytes>
S: [ID:b] OK\r\n
S: [ID:a] EMPTY\r\n
S: [ID:a] OK\r\n

# 4. Cluster inspection
C: NODE LIST\r\n
S: NODE id=1 region=local status=ACTIVE rtt_ms=3 addr=localhost port=4444\r\n
S: NODE id=2 region=iad status=ACTIVE rtt_ms=42 addr=iad.example port=4444\r\n
S: OK\r\n
```

## 12. Related documents

- `docs/reference/commands.md` — detailed semantics for each command accepted by the socket.
- `docs/reference/consensus.md` — explains how socket commands map onto the WPaxos-based replication layer.
