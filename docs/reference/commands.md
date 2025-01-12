# Database Wire Protocol Specification

## Abstract

This document specifies the text-based wire protocol for communication between clients and the distributed database system "Chronalys." The protocol supports a simplified and efficient communication model, ensuring compatibility across WAN and LAN environments. It provides guidelines for command structures, response formats, and SQL extensions for enhanced consistency, scalability, and optimized data operations.

This specification addresses both the technical and practical aspects of client-server communication, offering a robust framework for achieving efficient distributed database operations. By adopting this protocol, systems can ensure secure, reliable, and performant interactions, catering to diverse application needs and deployment scenarios.

---

## 1. Terminology

**Client**: A process that sends requests to the database server and receives corresponding responses.

**Server**: A process that receives client requests, processes them, and returns appropriate responses.

**Command**: A structured textual instruction sent by the client to request an operation from the server.

**Response**: The server's reply to a client command, providing results or status information.

**Frame**: A single communication unit, either a command or response, adhering to the protocol’s format.

**Stream**: A mechanism for returning large datasets incrementally, allowing clients to fetch results in manageable chunks.

---

## 2. Protocol Overview

The protocol is a line-based text protocol utilizing UTF-8 encoding. Commands and responses are exchanged as discrete frames, each terminated by a carriage return and newline sequence (\r\n). This format ensures compatibility and ease of parsing across various platforms and programming environments.

Each frame is designed to be lightweight and human-readable, simplifying debugging and interaction. Commands are synchronous by default, with support for asynchronous extensions where applicable. Fields within a frame are separated by whitespace, ensuring clear delineation of arguments.

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

    - Upon successful authentication, the server sends a `READY` frame, signaling that the session is active and commands can be issued.

   **Example:**
   ```
   Server: READY\r\n
   ```

### 3.2 Command Execution

Commands issued by clients are processed by the server in real-time. The server returns responses immediately unless the command specifies a deferred operation. Communication remains stateless unless explicitly required by the application context.

---

## 4. Frame Structure

### 4.1 General Frame Format

```
<COMMAND> <ARGUMENTS>\r\n
```

Commands consist of a keyword (`COMMAND`) followed by space-separated arguments. Responses adhere to the same structure, ensuring consistency and simplicity.

### 4.2 Command Frames

1. **Prepare a Query:**
   ```
   PREPARE [ID] [RAW QUERY]\r\n
   ```

2. **Execute a Prepared Query:**
   ```
   EXECUTE [ID]\r\n
   ```

3. **Perform a One-Off Query:**
   ```
   QUERY [RAW QUERY]\r\n
   ```

4. **Remove a Prepared Query:**
   ```
   FINALIZE [ID]\r\n
   ```

5. **Bind a Value to a Prepared Query:**
   ```
   BIND [ID] [PARAM] [TYPE] [VALUE]\r\n
   ```

6. **Transaction Commands:**
   ```
   BEGIN IMMEDIATE\r\n
   COMMIT\r\n
   ROLLBACK\r\n
   SAVEPOINT [name]\r\n
   RELEASE [name]\r\n
   ```

7. **Pragmas:**
   ```
   PRAGMA [name]=[VALUE]\r\n
   PRAGMA [name]\r\n
   ```
   Pragmas are used to query or configure node-specific settings.

8. **Principle Command:**
   ```
   PRINCIPLE [principle_name] [id]\r\n
   ```
   Sets the principle context for row-level security. A valid principle must be established before interacting with tables that use row-level security.

---

## 5. Response Frames

### 5.1 General Response Format

```
<STATUS> <MESSAGE>\r\n
```

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
   ROW ROWNUM TYPE VALUE\r\n
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

Heartbeat intervals are configurable, allowing flexibility based on application requirements.

---

## 7. SQL Specification Changes

### 7.1 CREATE TABLE with Replication Levels

```sql
CREATE [LOCAL|REGIONAL|GLOBAL] TABLE <table_name> (<schema_definition>)
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

Tables can enforce row-level security by associating rows with a `principle`. To enable this, the table must specify a shard by principle during creation.

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

1. Incorporating binary encoding for enhanced throughput.
2. Expanding protocol negotiation capabilities.
3. Introducing advanced query planning optimizations.

---

## 10. Security Considerations

1. **Encryption**: While TLS is unnecessary for local Unix pipes, it is strongly recommended for any network communication to prevent eavesdropping.
2. **Validation**: All inputs should be validated rigorously to prevent injection attacks or malformed queries.
3. **Access Control**: Ensure robust authentication and role-based authorization mechanisms.
