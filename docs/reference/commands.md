# Database Wire Protocol Specification

## Abstract

This document specifies the text-based wire protocol for communication between clients and the distributed database
system "Chronalys." The protocol supports a simplified and efficient communication model, ensuring compatibility across
WAN and LAN environments. It provides guidelines for command structures, response formats, and SQL extensions for
enhanced consistency, scalability, and optimized data operations.

This specification addresses both the technical and practical aspects of client-server communication, offering a robust
framework for achieving efficient distributed database operations. By adopting this protocol, systems can ensure secure,
reliable, and performant interactions, catering to diverse application needs and deployment scenarios. This includes
flexible query execution, dynamic configuration, and robust error handling mechanisms that empower both developers and
administrators to maintain highly available and resilient systems.

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

**Principle**: A context identifier used in row-level security to associate operations with a specific client or user
context. Principles ensure fine-grained access control for sharded tables.

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

### 3.1 Session Establishment

1. **Handshake**:

    - The server initiates communication with a `WELCOME` frame, specifying the protocol version and server details.
    - The client responds with a `HELLO` frame, providing its protocol version and unique identifier.

   **Example:**
   ```
   Server: WELCOME 1.0 Chronalys/1.0\r\n
   Client: HELLO 1.0 ClientID=xyz123\r\n
   ```

2. **Authentication** (Optional):

    - If authentication is required, the server sends an `AUTH REQUIRED` frame.
    - The client responds with an `AUTH` frame containing the necessary credentials.

   **Example:**
   ```
   Server: AUTH REQUIRED\r\n
   Client: AUTH USER=admin PASSWORD=secret\r\n
   ```

3. **Session Ready**:

    - Upon successful authentication, the server sends a `READY` frame, signaling that the session is active and
      commands can be issued.

   **Example:**
   ```
   Server: READY\r\n
   ```

### 3.2 Command Execution

Commands issued by clients are processed by the server in real-time. The server returns responses immediately unless the
command specifies a deferred operation. Communication remains stateless unless explicitly required by the application
context.

The protocol ensures that commands and their responses are tightly coupled, reducing ambiguity and simplifying
troubleshooting. By leveraging structured error handling, clients can quickly identify and address issues.

---

## 4. Frame Structure

### 4.1 General Frame Format

```
<COMMAND> <ARGUMENTS>\r\n
```

Commands consist of a keyword (`COMMAND`) followed by space-separated arguments. Responses adhere to the same structure,
ensuring consistency and simplicity. Each frame is independently parseable, minimizing the need for context-specific
rules.

### 4.2 Command Frames

1. **Prepare a Query:**
   ```
   PREPARE [ID] [RAW QUERY]\r\n
   ```
   Prepares a query for execution, associating it with an identifier.

2. **Execute a Prepared Query:**
   ```
   EXECUTE [ID]\r\n
   ```
   Executes a previously prepared query.

3. **Perform a One-Off Query:**
   ```
   QUERY [RAW QUERY]\r\n
   ```
   Executes a single query without preparation.

4. **Remove a Prepared Query:**
   ```
   FINALIZE [ID]\r\n
   ```
   Removes a previously prepared query from memory.

5. **Bind a Value to a Prepared Query:**
   ```
   BIND [ID] [PARAM] [TYPE] [VALUE]\r\n
   ```
   Binds a value to a parameter in a prepared query.

6. **Transaction Commands:**
   ```
   BEGIN IMMEDIATE\r\n
   COMMIT\r\n
   ROLLBACK\r\n
   SAVEPOINT [name]\r\n
   RELEASE [name]\r\n
   ```
   Manages database transactions, including savepoints for partial rollbacks.

7. **Pragmas:**
   ```
   PRAGMA [name]=[VALUE]\r\n
   PRAGMA [name]\r\n
   ```
   Configures or queries node-specific settings.

8. **Principle Command:**
   ```
   PRINCIPLE [principle_name] [id]\r\n
   ```
   Sets the principle context for row-level security. A valid principle must be established before interacting with
   tables that use row-level security.

9. **Scroll Command:**
   ```
   SCROLL [StreamID] [Count]\r\n
   ```
   Fetches a specific number of rows from a stream identified by `StreamID`.

10. **Scan Command:**
   ```
   SCAN [prefix]\r\n
   ```
   Performs a distributed prefix scan across all nodes in the cluster, returning all keys that match the specified prefix and are owned by any node. The results are returned in sorted lexicographic order.

   **Response Format:**
   - Empty result:
     ```
     EMPTY\r\n
     OK\r\n
     ```
   - With results:
     ```
     KEYS:<count>\r\n
     <key1>\r\n
     <key2>\r\n
     ...\r\n
     OK\r\n
     ```

   **Example:**
   ```
   Client: SCAN table:USERS:\r\n
   Server: KEYS:2\r\n
           table:USERS:row:ALICE\r\n
           table:USERS:row:BOB\r\n
           OK\r\n
   ```

   **Behavior:**
   - Broadcasts request to all cluster nodes
   - Each node scans its local data store for matching keys
   - Ownership filtering ensures only keys owned by each node are returned
   - Results are aggregated, deduplicated, and sorted before returning to client
   - Returns empty set if no matching keys are found or all nodes fail

   **Error Handling:**
   - If all nodes fail to respond: `ERROR <combined error messages>`
   - Partial failures are logged but do not affect the response if any nodes succeed

11. **Binary Data Commands:**

   **Set Binary Data:**
   ```
   KEY BLOB SET [key] [length]\r\n
   [binary-data-exactly-length-bytes]
   ```
   Stores binary data under the specified key. The `[length]` parameter specifies the exact number of bytes to read following the command line. Binary data may contain any bytes, including newlines (`\r\n`), null bytes, or other control characters, without affecting protocol parsing.

   **Example:**
   ```
   Client: KEY BLOB SET user:avatar:alice 1024\r\n
           [1024 bytes of binary image data]\r\n
   Server: OK\r\n
   ```

   **Get Binary Data:**
   ```
   KEY BLOB GET [key]\r\n
   ```
   Retrieves binary data stored under the specified key.

   **Response Format:**
   - Missing key (key not found):
     ```
     EMPTY\r\n
     OK\r\n
     ```
   - With data (including zero-length blobs):
     ```
     BLOB [length]\r\n
     [binary-data-exactly-length-bytes]
     OK\r\n
     ```

   **Note:** A zero-length blob (`BLOB 0\r\n`) is distinct from a missing key (`EMPTY\r\n`). This allows clients to differentiate between "key exists with empty data" and "key does not exist".

   **Example:**
   ```
   Client: KEY BLOB GET user:avatar:alice\r\n
   Server: BLOB 1024\r\n
           [1024 bytes of binary image data]
           OK\r\n
   ```

   **Behavior:**
   - Maximum binary data size is 128MB (configurable via `maxScannerLength`)
   - Binary data is read/written using exact byte counts, independent of content
   - No encoding or escaping is performed on binary data
   - Protocol remains line-based for commands, with binary payload following

   **Use Cases:**
   - Storing images, documents, or other binary files
   - Encrypted data that may contain arbitrary byte sequences
   - Serialized protocol buffers or other binary formats
   - Any data containing newlines or control characters

   **Error Handling:**
   - Invalid length (negative, non-numeric, or exceeds maximum): `ERROR <error message>`
   - Insufficient data available: `ERROR failed to read binary data`
   - General storage errors follow standard error response format

---

## 5. Response Frames

### 5.1 General Response Format

```
<STATUS> <MESSAGE>\r\n
```

Responses include a status code and an optional message. Additional metadata or data may follow in subsequent frames.

### 5.2 Common Status Codes

1. `OK`: Indicates successful execution.
2. `ERROR`: Indicates failure, optionally with severity levels (INFO, WARN, ERROR).
3. `AUTH_FAILED`: Indicates authentication failure.

### 5.3 Query Responses

Query results are returned incrementally to optimize performance and minimize memory overhead:

1. **Metadata:**
   ```
   STREAM <StreamID>\r\n
   META COLUMN_COUNT X\r\n
   META COLUMN_NAME X NAME\r\n
   OK\r\n
   ```
2. **Row Fetching:**
   ```
   SCROLL <StreamID> <Count>\r\n
   ROW COLNUM TYPE VALUE\r\n
   ```
3. **Final Metadata:**
   ```
   META LAST_INSERT_ID ID\r\n
   META ROWS_AFFECTED Y\r\n
   ```

---

## 6. Extended Features

### Heartbeats

To ensure the connection remains active, the protocol supports a heartbeat mechanism:

1. **Server-Initiated Ping:**
   ```
   PING\r\n
   ```
2. **Client Response:**
   ```
   PONG\r\n
   ```

Heartbeat intervals are configurable, allowing flexibility based on application requirements. Failure to respond to a
heartbeat can result in connection termination.

---

## 7. SQL Specification Changes

### 7.1 CREATE TABLE with Replication Levels

```sql
CREATE
[LOCAL|REGIONAL|GLOBAL] TABLE <table_name> (<schema_definition>)
```

- Example:
  ```
  CREATE LOCAL TABLE Users (id INT PRIMARY KEY, name TEXT);
  ```
- Replication defaults to `GLOBAL` if unspecified.

### 7.2 Table Groups

To ensure co-location and consistency, tables, views, and triggers can belong to a table group.

- **Syntax:**
  ```
  CREATE TABLE <table_name> (<schema_definition>) GROUP <group_name>;
  ALTER TABLE <table_name> ADD GROUP <group_name>;
  ALTER TABLE <table_name> DROP GROUP <group_name>;
  ```

### 7.3 Row-Level Security with Principles

Tables can enforce row-level security by associating rows with a `principle`. To enable this, the table must specify a
shard by principle during creation.

- **Syntax:**
  ```sql
  CREATE TABLE <table_name> (<schema_definition>) SHARD BY PRINCIPLE <principle_name>;
  ```
    - Example:
      ```sql
      CREATE TABLE users (id INT PRIMARY KEY, name TEXT) SHARD BY PRINCIPLE user_id;
      ```

- **Principle Command:**
  Before querying or interacting with a sharded table, the client must establish the principle context:
  ```
  PRINCIPLE <principle_name> <id>;
  ```

- **Error Handling:**
  Attempting to query or modify a sharded table without an appropriate `PRINCIPLE` results in a fatal error:
  ```
  ERROR ERROR Missing or invalid principle context.
  ```

### 7.4 Custom Pragmas

1. **PRAGMA CURRENT_NODE:** Outputs the ID of the current node.
   ```sql
   PRAGMA CURRENT_NODE;
   ```
   Response:
   ```
   NODE_ID = node123
   ```

2. **PRAGMA OWNED_TABLES:** Lists tables owned by the current node.
   ```sql
   PRAGMA OWNED_TABLES;
   ```
   Response:
   ```
   TABLES = [Users, Orders]
   ```

---

## 8. Error Handling

### Client-Side Errors

- `SYNTAX_ERROR`: Indicates malformed client commands.
  ```
  ERROR SYNTAX_ERROR\r\n
  ```

### Server-Side Errors

- `INTERNAL_ERROR`: Reports server-side failures.
  ```
  ERROR INTERNAL_ERROR\r\n
  ```

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
