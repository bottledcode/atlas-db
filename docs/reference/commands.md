# Database Wire Protocol Specification

## Abstract

This document specifies the text-based wire protocol used by Atlas Edge Database to expose its administrative and
key/value command surface. Atlas targets edge deployments spanning WAN links, so the protocol favours low ceremony,
human-readable frames while still supporting concurrency and binary payload transfer. The material below captures the
behaviour implemented in `atlas/socket` as of the current release and is intended to guide client implementers and
operators.

Where prior drafts referenced SQL-style wire commands, Atlas currently focuses on the key/value and cluster management
operations that exist in the codebase. Future SQL extensions will be documented separately once they ship.

---

## 1. Terminology

**Client**: A process that sends requests to the database server and receives corresponding responses. Clients interface
with the server using a structured command-response mechanism defined by this protocol.

**Server**: A process that receives client requests, processes them, and returns appropriate responses. The server
coordinates data consistency, replication, and concurrency.

**Command**: A structured textual instruction sent by the client to request an operation from the server. Commands can
range from simple data queries to administrative actions.

**Response**: The server's reply to a client command, providing results or status information. Responses adhere to a
well-defined format for clarity and consistency.

**Frame**: A single communication unit, either a command or response, adhering to the protocol’s format. Frames are the
foundational building blocks of communication in this protocol.

**Stream**: A mechanism for returning large datasets incrementally, allowing clients to fetch results in manageable
chunks. Streams are designed to optimize performance in distributed environments.

**Principal**: The logical identity attached to a socket session. Atlas forwards this value via gRPC metadata so that
downstream services (consensus, ACL evaluation) can apply access control and auditing.

---

## 2. Protocol Overview

The protocol is a line-based text protocol utilizing UTF-8 encoding. Commands and responses are exchanged as discrete
frames, each terminated by a carriage return and newline sequence (\r\n). This format ensures compatibility and ease of
parsing across various platforms and programming environments.

Each frame is designed to be lightweight and human-readable, simplifying debugging and interaction. Commands are
synchronous by default, with support for asynchronous extensions where applicable. Fields within a frame are separated
by whitespace, ensuring clear delineation of arguments.

Advanced features, such as row-level security and data streaming, integrate seamlessly into this protocol, offering
extended functionality without compromising simplicity.

---

## 3. Communication Model

### 3.1 Asynchronous Processing

The protocol supports both synchronous and asynchronous command execution to maximize throughput and handle multiple
concurrent operations.

**Request ID Tagging:**

- Commands can include an optional request ID prefix: `[ID:xxx] COMMAND`
- All responses for that command will include the same ID: `[ID:xxx] RESPONSE`
- Commands without request IDs are processed synchronously (backward compatible)
- Commands with request IDs are processed asynchronously in separate goroutines

**Benefits:**

- **Concurrent Execution**: Multiple commands can execute simultaneously
- **Response Correlation**: Clients can match responses to requests even when out-of-order
- **Interleaved Streaming**: Large data streams from different commands don't block each other
- **Backward Compatible**: Existing clients without request IDs work unchanged

**Example:**

```
Client: [ID:1] KEY BLOB GET large_file_1\r\n
Client: [ID:2] KEY BLOB GET large_file_2\r\n
Client: [ID:3] SCAN table:USERS:\r\n
Server: [ID:3] KEYS:2\r\n
        table:USERS:row:ALICE\r\n
        table:USERS:row:BOB\r\n
        [ID:3] OK\r\n
Server: [ID:1] BLOB 1048576\r\n
        [1MB of binary data]
        [ID:1] OK\r\n
Server: [ID:2] BLOB 2097152\r\n
        [2MB of binary data]
        [ID:2] OK\r\n
```

**Implementation Notes:**

- Request IDs are arbitrary strings (avoid `]` character)
- Responses may arrive in any order
- Clients must track outstanding requests
- Server processes tagged commands concurrently
- Thread-safe write operations ensure response integrity

### 3.2 Session Establishment

1. **Handshake**:

    - The server initiates communication with a `WELCOME` frame, specifying the protocol version and server details.
    - The client responds with a `HELLO` frame, providing its protocol version and unique identifier.

   **Example:**
   ```
   Server: WELCOME 1.0 Chronalys/1.0\r\n
   Client: HELLO 1.0 clientId=cli-example\r\n
   ```

2. **Session Ready**:

    - Upon successful authentication, the server sends a `READY` frame, signaling that the session is active and
      commands can be issued.

   **Example:**
   ```
   Server: READY\r\n
   ```

`Chronalys/1.0` is the historic server identifier carried forward for compatibility; the software is Atlas Edge
Database.

### 3.3 Command Execution

Commands issued by clients are processed by the server in real-time. The execution model depends on whether the command
includes a request ID:

**Synchronous Mode (No Request ID):**

- Server processes one command at a time
- Response guaranteed to arrive before next command is processed
- Backward compatible with existing clients
- Commands and responses are tightly coupled in order

**Asynchronous Mode (With Request ID):**

- Server spawns a goroutine for each tagged command
- Multiple commands execute concurrently
- Responses may arrive in any order
- Client must correlate responses using request IDs
- Ideal for I/O-bound operations and bulk transfers

The protocol ensures clear command-response correlation through request ID tagging, reducing ambiguity even when
responses arrive out-of-order. By leveraging structured error handling and thread-safe writes, clients can quickly
identify and address issues while maintaining high throughput.

---

## 4. Frame Structure

### 4.1 General Frame Format

```
<COMMAND> <ARGUMENTS>\r\n
```

Commands consist of a keyword (`COMMAND`) followed by space-separated arguments. Responses adhere to the same structure,
ensuring consistency and simplicity. Each frame is independently parseable, minimizing the need for context-specific
rules.

### 4.2 Command Families

1. **Key Reads**
   ```
   KEY GET <key>\r\n
   ```
   Returns `VALUE:<bytes>` when the key exists, `NOT_FOUND` otherwise. Keys use dotted notation (for example,
   `table.row.attr`). Atlas normalises tokens to uppercase for routing but preserves the raw key when writing to the
   underlying key builder.

2. **Key Writes**
   ```
   KEY PUT <key> [PRINCIPAL <name>] [WORM TTL <duration>|WORM EXPIRES <timestamp>] <value...>\r\n
   KEY SET <key> ...                         ; alias for KEY PUT
   ```
   Stores UTF-8 values. Optional modifiers capture the acting principal for observability and record write-once (WORM)
   metadata. Duration strings follow Go’s `time.ParseDuration`, timestamps use RFC3339 format. Failures return
   `ERROR WARN` with the parsing issue.

3. **Key Deletes**
   ```
   KEY DEL <key>\r\n
   ```
   Propagates a delete through consensus. Successful operations reply with `OK`; otherwise the error from the quorum is
   surfaced via `ERROR WARN`.

4. **Binary Writes**
   ```
   KEY BLOB SET <key> <length>\r\n
   <binary payload bytes>
   ```
   Reads exactly `<length>` bytes after the command line and writes them as-is. The maximum accepted size is 128 MiB.

5. **Binary Reads**
   ```
   KEY BLOB GET <key>\r\n
   ```
   Emits `BLOB <length>` + payload + `OK` when present, or `EMPTY` + `OK` when the key is absent.

6. **Prefix Scan**
   ```
   SCAN <table[.row-prefix]>\r\n
   ```
   Broadcasts a prefix scan across the cluster and returns `KEYS:<count>` followed by each key on its own line. The
   command terminates with `OK`. Errors during quorum selection or RPC fan-out are returned via `ERROR WARN`.

7. **Reserved Commands**
   ```
   COUNT ...
   SAMPLE ...
   ```
   Accepted but currently respond with `ERROR WARN <not implemented>`.

8. **Principal Management**
   ```
   PRINCIPAL WHOAMI\r\n
   PRINCIPAL ASSUME <name>\r\n
   ```
   `ASSUME` validates and stores the session principal; `WHOAMI` reports it (or `(none)`). Names must be 1–256 runes
   without control characters.

9. **Access Control**
   ```
   ACL GRANT <table> <principal> PERMS <READ|WRITE|OWNER>\r\n
   ACL REVOKE <table> <principal> PERMS <READ|WRITE|OWNER>\r\n
   ```
   Both commands write migrations through consensus to add or remove permissions tied to the supplied dotted key.

10. **Node Introspection**
    ```
    NODE LIST\r\n
    NODE INFO <id>\r\n
    NODE PING <id>\r\n
    ```
    `LIST` enumerates nodes known to metadata, `INFO` prints details for a specific node, and `PING` performs a gRPC
    round-trip using the node-connection manager, returning `PONG node=<id> rtt_ms=<millis>`.

11. **Quorum Inspection**
    ```
    QUORUM INFO <table>\r\n
    ```
    Describes the Q1 and Q2 quorums Atlas would form for the named table, printing participating nodes and sizes.

12. **Future Surface Area**
    Historical drafts referenced additional commands (`PRAGMA`, `QUERY`, `SCROLL`, SQL DDL). These are not currently
    implemented in Atlas. Clients should not depend on them until the features land and updated documentation is
    published.

---

## 5. Response Frames

### 5.1 General Format

Atlas responses are line-oriented like requests. A successful command sends any data frames first and concludes with
`OK\r\n`. Errors are reported as `ERROR <LEVEL> <message>\r\n`; no trailing `OK` follows an error.

### 5.2 Status Levels

- `OK` – command completed successfully.
- `ERROR WARN` – recoverable failure (validation error, not found, permission denied). Connection remains open.
- `ERROR INFO` – reserved for streaming hints; unused today but accepted by clients for forward compatibility.
- `ERROR FATAL` – unrecoverable protocol violation. Atlas closes the socket immediately after emitting the frame.

### 5.3 Data Frames

- `VALUE:<bytes>` – returned by `KEY GET` when a UTF-8 value exists.
- `NOT_FOUND` – returned by `KEY GET` when the key is absent.
- `KEYS:<count>`, followed by `<key>` lines – returned by `SCAN`.
- `BLOB <length>` – header emitted prior to binary payloads from `KEY BLOB GET`. The payload is streamed verbatim and
  followed by `OK`.
- `EMPTY` – used by `KEY BLOB GET` when a key is missing or mapped to a nil payload.
- `PRINCIPAL <name>` – emitted by `PRINCIPAL WHOAMI`.
- `NODE ...`, `Q1 ...`, `Q2 ...`, `PONG ...`, `ACL granted/revoked ...` – human-readable summaries described alongside
  each command family above.

---

## 6. Extended Features

### Heartbeats

A heartbeat mechanism has been contemplated in earlier designs but is not implemented in `atlas/socket`. Clients should
assume the server will remain silent unless responding to issued commands. Liveness can be inferred by sending benign
commands such as `PRINCIPAL WHOAMI` when necessary.

---

## 7. SQL and Extended Surface Area

Atlas does not currently expose SQL statements, pragmas, or streaming row sets over the socket. These capabilities may
arrive in future releases and will be documented when available. References to `PRAGMA`, `SCROLL`, or SQL DDL in earlier
drafts should be treated as historical notes, not supported behaviour.

---

## 8. Error Handling

Atlas centralises error mapping in `socket.formatCommandError`. Understanding the mapping helps clients decide when to
retry.

### Client Input Issues

- Invalid arity (e.g. missing arguments) triggers `ERROR WARN <usage message>`.
- Invalid numeric conversions (`KEY BLOB SET foo abc`) return `ERROR WARN invalid length...`.
- Principal validation failures reply with `ERROR WARN usage: PRINCIPAL ASSUME <name>`.

### Server or Consensus Failures

- gRPC status codes are translated to readable messages, e.g. `ERROR WARN permission denied for principal 'alice': ...`
  or `ERROR WARN service unavailable: ...`.
- Fatal protocol violations (malformed handshake, oversized frame) emit `ERROR FATAL ...` and close the connection.
- Unexpected panics or I/O errors during writes also close the socket; clients should treat EOF as a failed command.

---

## 9. Future Work

1. ~~Incorporating binary encoding for enhanced throughput.~~ ✓ Implemented via `KEY BLOB SET/GET` commands
2. Expanding protocol negotiation capabilities.
3. Introducing advanced query planning optimizations.
4. Implementing automatic chunking at the driver level for data exceeding size limits.

---

## 10. Security Considerations

1. **Encryption**: While TLS is unnecessary for local Unix pipes, it is strongly recommended for any network
   communication to prevent eavesdropping.
2. **Validation**: All inputs should be validated rigorously to prevent injection attacks or malformed queries.
3. **Access Control**: Ensure robust authentication and role-based authorization mechanisms.
