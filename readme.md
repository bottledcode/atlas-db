# Atlas Edge Database

Atlas is an edge-oriented, strongly consistent key-value store that runs across wide-area networks. Each table has a
single owner elected through a WPaxos-inspired consensus layer, and ownership can move to wherever the workload is. The
project bundles a Caddy module, a Unix-socket command protocol, and tooling for bootstrapping new clusters.

## Highlights
- WAN-friendly consensus with tunable quorum tolerances (`Fn`/`Fz`).
- Table-level ownership, grouping, and migration support.
- Text-based administrative interface over a Unix socket (documented in `docs/reference`).
- Native binary/object storage via `KEY BLOB` commands.
- Caddy integration for TLS termination, bootstrap APIs, and an interactive REPL (`caddy atlas`).

## Project status
Atlas is under active development. APIs and on-disk formats may still change, and some features (such as write-once (WORM)
enforcement and SQL surface area) are in flight. See the various `*-plan.md` files in the repository for design notes.

## Prerequisites
- Go 1.25 or newer (as declared in `go.mod`).
- `protoc` plus the Go codegen plugins (`make caddy`/`make atlasdb` will install them into `tools/`).
- C toolchain for linking BadgerDB (installed automatically by the Go toolchain on most platforms).

## Building
The Makefile bundles the usual developer workflows:

```sh
make caddy       # build the Atlas-enabled caddy binary in ./caddy
make atlasdb     # format, lint, and produce the atlasdb binary (stripped caddy build)
```

Artifacts are copied into the repository root:
- `./caddy` – Caddy with the Atlas module compiled in.
- `./atlasdb` – identical binary intended for packaging/distribution.

## Running a local node
1. Build the Caddy binary (`make caddy`).
2. Start a seed node using the sample configuration:
   ```sh
   ./caddy run --config atlas/caddy/Caddyfile2
   ```
   This creates data in `/tmp/atlas2/` and exposes the Unix socket at `/tmp/atlas2/socket`.
3. (Optional) Start an additional node with `atlas/caddy/Caddyfile`, pointing its `connect` directive at the first node.

### Example Caddyfile
The bundled templates in `atlas/caddy/` are a good starting point. A minimal single-node configuration looks like this:

```caddyfile
{
    admin off
    auto_https disable_redirects
}

https://localhost:4444 {
    atlas {
        advertise localhost:4444
        region local
        credentials mySecret
        db_path /tmp/atlas2/
        socket /tmp/atlas2/socket
        # connect https://bootstrap-host:port   # uncomment to join an existing cluster
    }
    tls internal
}
```

Adjust the `db_path`/`socket` locations to suit your environment. When joining a cluster, replace the commented `connect`
line with the bootstrap node address and ensure `credentials` matches the remote node’s expectation.

## Talking to the socket
Use the bundled REPL:

```sh
./caddy atlas /tmp/atlas2/socket
> PRINCIPAL ASSUME alice
OK
> KEY PUT sample.row hello world
OK
> KEY GET sample.row
VALUE:hello world
OK
> QUORUM INFO atlas.nodes
Q1 size=1
id=1 region=local addr=localhost port=4444
Q2 size=1
id=1 region=local addr=localhost port=4444
OK
```

All socket commands, their responses, and binary framing rules are documented in `docs/reference/protocol.md` and
`docs/reference/commands.md`.

## Working with keys and tables
Atlas uses dotted keys to determine data placement. The segment before the first dot is the table name, everything
after it belongs to the row (and any optional suffix). Tables are the unit of ownership the consensus layer migrates,
so every key that shares a table name is guaranteed to live together.

- `KEY PUT users.alice profile-json` writes the `alice` row in the `users` table.
- `KEY PUT users.bob profile-json` lands in the same `users` table, so the two records always co-migrate.
- `SCAN users` returns every row in the `users` table; `SCAN users.al` would restrict the results to rows prefixed by
  `al`.

Because the table is indivisible, you can cluster related keys (per-customer data, per-tenant settings, session state)
and rely on Atlas to move them together when the write hotspot changes. This makes features such as colocated chat
conversations or sharded document stores practical without having to manually coordinate rebalancing.

### Multi-tenant session example
```sh
./caddy atlas /tmp/atlas2/socket
> PRINCIPAL ASSUME alice
OK
> KEY PUT users.alice {"session":"s1"}
OK
> PRINCIPAL ASSUME bob
OK
> KEY PUT users.bob {"session":"s2"}
OK
> SCAN users
KEYS:2
users.alice
users.bob
OK
```

Both user records share the same table and therefore follow each other around the cluster. Downstream services (such as
authorization hooks or analytics pipelines) can rely on that property when scheduling work or caching.

## Testing
Run the full validation suite (unit tests, race detector, integration tests, and ACL end-to-end script) with:

```sh
make test
```

Some integration scenarios expect multiple nodes or rely on the generated Caddy binary; review test logs for guidance.

## Documentation
- Reference docs: `docs/reference/`
- Design notes and outstanding work: `*-plan.md`
- Consensus internals: `docs/reference/consensus.md`

## License
Atlas Edge Database is distributed under the GNU Affero General Public License v3.0. See `LICENSE` for the full text.

![Alt](https://repobeats.axiom.co/api/embed/c76f05a70d3f12ea0e927f16919d7cae3580bac4.svg "Repobeats analytics image")
