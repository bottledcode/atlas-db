/*
 * This file is part of Atlas-DB.
 *
 * Atlas-DB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Atlas-DB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Atlas-DB. If not, see <https://www.gnu.org/licenses/>.
 *
 */

package main

import (
	"bufio"
	"context"
	_ "embed"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/bootstrap"
	"go.uber.org/zap"
	"os"
	"strings"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitemigration"
)

func parseOptions(opts []string, defaults *atlas.Options) (opt *atlas.Options) {
	if len(opts) == 0 {
		return defaults
	}

	opt = defaults

	for i, field := range opts {
		value := ""
		if strings.Contains(field, "=") {
			value = strings.Split(field, "=")[1]
			field = strings.Split(field, "=")[0]
		} else {
			if i+1 < len(opts) {
				value = opts[i+1]
			}
		}
		if strings.HasPrefix(field, "--") {
			switch field {
			case "--db":
				opt.DbFilename = value
			case "--meta":
				opt.MetaFilename = value
			case "--reset":
				opt.DoReset = true
			case "--connect":
				opt.BootstrapConnect = value
			}
		}
	}

	return
}

func main() {
	fmt.Println("Simple REPL for Testing SQLite")
	fmt.Println("Type 'exit' to quit")
	var err error

	atlas.Logger, err = zap.NewDevelopment()
	if err != nil {
		fmt.Println("Error creating logger:", err)
		return
	}

	atlas.CurrentOptions = parseOptions(os.Args[1:], &atlas.Options{
		DbFilename:   "/tmp/atlas.db",
		MetaFilename: "/tmp/atlas.meta",
		DoReset:      false,
	})

	if atlas.CurrentOptions.DoReset {
		os.Remove(atlas.CurrentOptions.DbFilename)
		os.Remove(atlas.CurrentOptions.MetaFilename)
		fmt.Println("Database reset")
		return
	}

	// Initialize Atlas
	if atlas.CurrentOptions.BootstrapConnect != "" {
		// we are connecting to a bootstrap server
		err = bootstrap.DoBootstrap(context.Background(), atlas.CurrentOptions.BootstrapConnect, atlas.CurrentOptions.MetaFilename)
		if err != nil {
			fmt.Println("Error bootstrapping:", err)
			return
		}
	}

	if _, err := os.Stat(atlas.CurrentOptions.MetaFilename); err != nil {
		f, err := os.Create(atlas.CurrentOptions.MetaFilename)
		if err != nil {
			fmt.Println("Error creating meta database:", err)
			return
		}
		f.Close()
	}

	// Start REPL loop
	repl(atlas.Pool)
}

func repl(conn *sqlitemigration.Pool) {
	reader := bufio.NewReader(os.Stdin)
	db, _ := conn.Take(context.Background())
	for {
		fmt.Print(">> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		input = strings.TrimSpace(input)
		if input == "exit" {
			break
		}
		processInput(input, db)
	}
}

func processInput(input string, conn *sqlite.Conn) {
	_, _ = atlas.ExecuteSQL(context.Background(), input, conn, true)
}
