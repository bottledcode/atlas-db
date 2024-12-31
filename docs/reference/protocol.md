# Protocol

The protocol is a simple text-based protocol used to communicate with the Atlas server over a Unix socket.

## Overview

The protocol is designed to be simple and easy to implement. It is a line-based protocol, with each line representing a
command or response.

## Responses

The server responds with `OK` if the command was successful or `ERROR` followed by a code and message if the command
failed.

### Query responses

Query results are returned in the following form:

```
META COLUMN_COUNT <N>
META COLUMN_NAME <N[x]> <NAME>
ROW <N[x]> <TYPE> <VALUE>
META LAST_INSERT_ID <ID>
META ROWS AFFECTED <N>
```

For example, for the following results:

| id | name |
|----|------|
| 1  | John |
| 2  | NULL |

The response would be:

```
META COLUMN_COUNT 2
META COLUMN_NAME 0 id
META COLUMN_NAME 1 name
ROW 0 INT 1
ROW 0 STRING John
ROW 1 INT 2
ROW 1 NULL
```

## Types

The following types are supported:

- `INT` where the value is a base-10 integer in ascii form.
- `FLOAT` where the value is a base-10 float in ascii form.
- `TEXT` where the value is a string.
- `NULL` where the value is not sent.
- `BLOB` where the value is a base64 encoded string.

## Commands

> Note: in the syntax below, `<...>` denotes a required parameter, `[...]` denotes an optional parameter. UPPERCASE
> denotes case-insensitivity and lowercase denotes case-sensitivity.

### Prepare

#### Syntax

```
PREPARE <ID> <sql>
```

#### Description

The `PREPARE` command is used to prepare a query for execution.
The ID is a case-insensitive and unique identifier for the query.
The SQL is the query to prepare and may contain placeholders.

#### Example

```
PREPARE 1 SELECT * FROM table WHERE id = ?\r\n
PREPARE USERS SELECT * FROM users WHERE id = :id\r\n
```

### Execute

#### Syntax

```
EXECUTE <ID>
```

#### Description

Executes the prepared query and clears any bound parameters.

#### Example

```
EXECUTE USERS\r\n
```

### FINALIZE

#### Syntax

```
FINALIZE <ID>
```

#### Description

Finalizes the prepared query and removes it from the server.

#### Example

```
FINALIZE 1\r\n
```

### BIND

#### Syntax

```
BIND <ID> <param> <TYPE> [value]
```

#### Description

Binds a value to a prepared query. The param can be either a positional parameter (number) or a named parameter (starting with ':'). 
The value must match the specified type. For NULL values, omit the value parameter.
#### Example

```
BIND 1 INT 1\r\n
BIND :name TEXT \r\n
```

### BEGIN

#### Syntax

```
BEGIN [IMMEDIATE|DEFERRED|EXCLUSIVE]
```

#### Description

Starts a new transaction.

#### Example

```
BEGIN IMMEDIATE\r\n
```

### COMMIT

#### Syntax

```
COMMIT
```

#### Description

Commits the current transaction.

#### Example

```
COMMIT\r\n
```

### ROLLBACK

#### Syntax

```
ROLLBACK [TO <SAVEPOINT>]
```

#### Description

Rolls back the current transaction or to a savepoint.

#### Example

```
ROLLBACK TO savepoint\r\n
```

### SAVEPOINT

#### Syntax

```
SAVEPOINT <name>
```

#### Description

Creates a new savepoint.

#### Example

```
SAVEPOINT savepoint\r\n
```

### RELEASE

#### Syntax

```
RELEASE <savepoint>
```

#### Description

Releases a savepoint.

#### Example

```
RELEASE savepoint\r\n
```

### PRAGMA

#### Syntax

```
PRAGMA <name> [value]
```

#### Description

Sets a pragma value.

#### Example

```
PRAGMA ATLAS_CONSISTENCY strong\r\n
```
