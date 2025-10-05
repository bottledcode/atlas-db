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

package commands

import (
	"context"
	"fmt"

	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/kv"
)

type ACLCommand struct {
	CommandString
}

func (c *ACLCommand) GetNext() (Command, error) {
	if next, ok := c.SelectNormalizedCommand(1); ok {
		switch next {
		case "GRANT":
			return &ACLGrantCommand{*c}, nil
		case "REVOKE":
			return &ACLRevokeCommand{*c}, nil
		}
		return EmptyCommandString, fmt.Errorf("ACL command expected GRANT or REVOKE, got %s", next)
	}
	return EmptyCommandString, fmt.Errorf("ACL command requires GRANT or REVOKE")
}

// ACLGrantCommand handles ACL GRANT <principal> <table> <key>
type ACLGrantCommand struct {
	ACLCommand
}

func (c *ACLGrantCommand) GetNext() (Command, error) {
	return c, nil
}

func (c *ACLGrantCommand) Execute(ctx context.Context) ([]byte, error) {
	// ACL GRANT <table> <name> PERMS <permissions>
	if err := c.CheckMinLen(6); err != nil {
		return nil, err
	}

	tableKey, _ := c.SelectNormalizedCommand(2)
	principal := c.SelectCommand(3)
	permsKeyword, _ := c.SelectNormalizedCommand(4)
	permissions := c.SelectCommand(5)

	if tableKey == "" || principal == "" || permsKeyword != "PERMS" || permissions == "" {
		return nil, fmt.Errorf("ACL GRANT requires format: ACL GRANT <table> <name> PERMS <permissions>")
	}

	key := kv.FromDottedKey(tableKey)

	switch permissions {
	case "READ":
		err := atlas.AddReader(ctx, key, principal)
		if err != nil {
			return nil, fmt.Errorf("failed to add reader: %w", err)
		}
	case "WRITE":
		err := atlas.AddWriter(ctx, key, principal)
		if err != nil {
			return nil, fmt.Errorf("failed to add writer: %w", err)
		}
	case "OWNER":
		err := atlas.AddOwner(ctx, key, principal)
		if err != nil {
			return nil, fmt.Errorf("failed to add owner: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown permissions: %s", permissions)
	}

	return fmt.Appendf(nil, "ACL granted to %s for %s with permissions %s", principal, key, permissions), nil
}

// ACLRevokeCommand handles ACL REVOKE <principal> <table> <key>
type ACLRevokeCommand struct {
	ACLCommand
}

func (c *ACLRevokeCommand) GetNext() (Command, error) {
	return c, nil
}

func (c *ACLRevokeCommand) Execute(ctx context.Context) ([]byte, error) {
	// ACL REVOKE <table> <name> PERMS <permissions>
	if err := c.CheckMinLen(6); err != nil {
		return nil, err
	}

	tableKey, _ := c.SelectNormalizedCommand(2)
	principal := c.SelectCommand(3)
	permsKeyword, _ := c.SelectNormalizedCommand(4)
	permissions := c.SelectCommand(5)

	if tableKey == "" || principal == "" || permsKeyword != "PERMS" || permissions == "" {
		return nil, fmt.Errorf("ACL REVOKE requires format: ACL REVOKE <table> <name> PERMS <permissions>")
	}

	key := kv.FromDottedKey(tableKey)
	switch permissions {
	case "READ":
		err := atlas.RevokeReader(ctx, key, principal)
		if err != nil {
			return nil, fmt.Errorf("failed to revoke reader: %w", err)
		}
	case "WRITE":
		err := atlas.RevokeWriter(ctx, key, principal)
		if err != nil {
			return nil, fmt.Errorf("failed to revoke writer: %w", err)
		}
	case "OWNER":
		err := atlas.RevokeOwner(ctx, key, principal)
		if err != nil {
			return nil, fmt.Errorf("failed to revoke owner: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown permissions: %s", permissions)
	}
	return fmt.Appendf(nil, "ACL revoked from %s for %s with permissions %s", principal, key, permissions), nil
}
