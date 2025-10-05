# Atlas Socket Protocol

Atlas exposes a text-based control channel over a Unix domain socket. The protocol is intentionally minimal so it can
be used from shell scripts, the embedded REPL (`caddy atlas`), or custom clients.

## Connection lifecycle

- **Transport**: Unix domain socket chosen through `atlas/options` (default: `atlas.sock`). Each accepted connection is
  served by `atlas/socket` with a five minute write deadline.
- **Handshake**:
    1. Server sends `WELCOME 1.0 Chronalys/1.0\r\n` immediately after accept.
    2. Client must reply with `HELLO 1.0 <client-id>\r\n`.
    3. Server responds `READY\r\n` and begins processing commands.
       If the client omits the handshake or sends an unsupported version, the server replies with
       `ERROR FATAL <message>\r\n` and closes the socket.
- **Authentication**: not yet implemented. The Atlas Caddy module relies on gRPC authentication in front of the socket
  when the node is exposed remotely.

## Frame format

- Commands and responses are ASCII text frames terminated with a carriage return and newline (`\r\n`).
- Tokens are separated by ASCII whitespace. The server normalises tokens to uppercase for routing but preserves the raw
  token stream for values.
- Empty lines are ignored; control characters are not permitted.

### Request identifiers

- Prefixing a command with `[ID:<token>]` turns it into an asynchronous request. `<token>` may contain any characters
  except `]`.
- Asynchronous commands execute concurrently. Every textual response line emitted by the server (including `OK`,
  `ERROR …`, and `BLOB …` headers) is prefixed with the same `[ID:<token>]` so clients can correlate out-of-order
  replies.
- Commands without a request identifier are processed synchronously; Atlas replies before reading the next command from
  that client.

## Responses

Atlas follows a consistent response pattern:

- **Successful command**: zero or more data frames followed by `OK\r\n` (e.g. `KEYS:3`, `PRINCIPAL alice`,
  `PONG node=2 rtt_ms=15`).
- **Error**: a single frame `ERROR <LEVEL> <message>\r\n`. The server never sends `OK` after an error. `LEVEL` is one
  of:
    - `WARN` – recoverable issue; the connection remains open.
    - `INFO` – informational streaming hint (reserved for future multi-part responses).
    - `FATAL` – unrecoverable error; the server closes the connection immediately after sending the frame.
- **Binary responses**: `BLOB <length>\r\n` (prefixed with the request ID when present) followed by `<length>` raw
  bytes,
  then `OK\r\n`. If no value exists the server emits `EMPTY\r\nOK\r\n` instead.

Error messages are sanitised so that control characters cannot inject extra frames.

## Binary payloads

`KEY BLOB SET` commands must be followed immediately by a binary payload whose size matches the declared length. No
terminating newline is required. The server writes the payload directly to storage and then responds with `OK`. Clients
should not pipeline additional commands until the payload has been transmitted.

## Principals and metadata propagation

`PRINCIPAL ASSUME <name>` updates the session principal. The name is trimmed, validated to be 1–256 runes, and stripped
of control characters. Subsequent commands run with gRPC metadata key `atlas-principal=<name>`, allowing downstream
services to enforce ACLs or perform auditing. `PRINCIPAL WHOAMI` reports the current value.

## Example interaction

```
Server: WELCOME 1.0 Chronalys/1.0\r\n
Client: HELLO 1.0 clientId=example\r\n
Server: READY\r\n
Client: PRINCIPAL ASSUME alice\r\n
Server: OK\r\n
Client: [ID:req-1] KEY GET table.row\r\n
Server: [ID:req-1] VALUE:payload\r\n
Server: [ID:req-1] OK\r\n
Client: KEY BLOB SET table.row 3\r\n<0x000102>
Server: OK\r\n
```

The REPL started via `./caddy atlas /path/to/socket` implements the handshake and maintains this framing behaviour. The
same framing applies to automated clients.
