package main

import (
	caddycmd "github.com/caddyserver/caddy/v2/cmd"

	_ "github.com/bottledcode/atlas-db/atlas/caddy/module"
	_ "github.com/caddyserver/caddy/v2/modules/standard"
)

func main() {
	caddycmd.Main()
}
