# Atlas Socket Command Reference

Atlas ships a text protocol for administrative and data-plane operations over the Unix socket exposed by each node. The
syntax is intentionally terse: tokens are space-delimited, keywords are case-insensitive, and commands end with `\r\n`.
Optional request identifiers (`[ID:token]`) follow the protocol rules described in `docs/reference/protocol.md`.

In the tables below, literals appear in UPPERCASE, parameters in angle brackets, and optional segments in brackets.

## Session management

### `PRINCIPAL WHOAMI`

- **Purpose**: print the principal currently associated with the session, or `(none)` when unset.
- **Response**: `PRINCIPAL <name>` followed by `OK`.

### `PRINCIPAL ASSUME <name>`

- **Purpose**: set the session principal. The value is trimmed, validated to be 1–256 runes, and must not contain
  control
  characters.
- **Response**: `OK` on success. Invalid inputs return `ERROR WARN usage: PRINCIPAL ASSUME <name>`.
- **Side-effects**: subsequent commands carry the principal via gRPC metadata key `atlas-principal`.

## Key-value operations

Keys use dotted notation (`table.row[.suffix…]`). Segments are normalised to uppercase before storage and map to the
internal prefix layout (`table:<TABLE>:row:<ROW>:...`).

### `KEY GET <key>`

- **Purpose**: fetch a UTF-8 value.
- **Response**:
    - `VALUE:<bytes>` followed by `OK` when a value exists (no separator between the colon and payload).
    - `NOT_FOUND` followed by `OK` when the key is missing.

### `KEY PUT <key> [PRINCIPAL <name>] [WORM TTL <duration>|WORM EXPIRES <timestamp>] <value…>`

`KEY SET` is accepted as an alias for `KEY PUT`.

- **Purpose**: store a UTF-8 value. The value is the remainder of the line after recognised modifiers.
- **Modifiers**:
    - `PRINCIPAL <name>` is logged with the write for auditing.
    - `WORM TTL <duration>` sets a proposed write-once retention window using Go duration syntax (for example `72h`).
    - `WORM EXPIRES <timestamp>` supplies an absolute RFC3339 expiry timestamp.
- **Response**: `OK` on success. Invalid durations/timestamps return `ERROR WARN <message>`.
- **Current behaviour**: WORM metadata is recorded in logs only; enforcement hooks are planned but not yet implemented.

### `KEY DEL <key>`

- **Purpose**: delete a key.
- **Response**: `OK` if the delete is broadcast successfully; `ERROR WARN <message>` if the quorum reports failure.

### `KEY BLOB SET <key> <length>`

- **Purpose**: write binary payloads.
- **Request body**: after sending the command line, stream `<length>` raw bytes with no extra newline.
- **Response**: `OK` once the payload is written. If the reader cannot parse `<length>` the server replies with
  `ERROR WARN invalid length…`.

### `KEY BLOB GET <key>`

- **Purpose**: read binary payloads.
- **Response**: `BLOB <length>` followed by `<length>` raw bytes and a trailing `OK`. When the key is missing the server
  returns `EMPTY` followed by `OK`.

## Discovery commands

### `SCAN <table[.row-prefix]>`

- **Purpose**: enumerate keys across the cluster. Supplying `foo` scans table `FOO`; `foo.bar` narrows the row prefix.
- **Response**: `KEYS:<count>` followed by one key per line, ending with `OK`.
- **Notes**: this command requires a healthy consensus quorum. When consensus resources are unavailable the server
  returns `ERROR WARN <reason>`.

### `COUNT …`, `SAMPLE …`

- **Purpose**: reserved for future extensions.
- **Response**: `ERROR WARN COUNT not implemented` / `ERROR WARN SAMPLE not implemented`.

## Cluster management

### `NODE LIST`

- **Purpose**: list nodes known to the metadata repository.
- **Response**: one line per node in the format
  `NODE id=<id> region=<region> status=<ACTIVE|INACTIVE> rtt_ms=<ms> addr=<host> port=<port>` followed by `OK`.

### `NODE INFO <id>`

- **Purpose**: retrieve details for a specific node ID.
- **Response**: a single summary line matching the `NODE LIST` format, then `OK`. Unknown IDs return `ERROR WARN`.

### `NODE PING <id>`

- **Purpose**: perform a gRPC ping via the node connection manager.
- **Response**: `PONG node=<id> rtt_ms=<ms>` followed by `OK`.

### `QUORUM INFO <table>`

- **Purpose**: inspect the quorums Atlas would use for the named table.
- **Response**:
    - `Q1 size=<n>` followed by the participating nodes (`id=<id> region=<name> addr=<host> port=<port>`).
    - `Q2 size=<n>` with the same formatting.
    - An `OK` terminator. Errors from quorum selection yield `ERROR WARN <reason>`.

## Access control

### `ACL GRANT <table> <principal> PERMS <READ|WRITE|OWNER>`

- **Purpose**: publish an ACL grant migration for the given table key and principal.
- **Response**: `ACL granted to <principal> for <key> with permissions <perm>` followed by `OK`.

### `ACL REVOKE <table> <principal> PERMS <READ|WRITE|OWNER>`

- **Purpose**: revoke previously granted permissions.
- **Response**: `ACL revoked from <principal> for <key> with permissions <perm>` followed by `OK`.

Both ACL commands operate on dotted keys (`table.row[...]`) and rely on the underlying consensus service to propagate
the changes.
