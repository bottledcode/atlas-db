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

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
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

	// Get the metadata store
	kvPool := kv.GetPool()
	if kvPool == nil {
		return nil, fmt.Errorf("KV pool not initialized")
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}

	// Convert dotted key to KeyBuilder format to match what ReadKey uses
	builder := kv.FromDottedKey(tableKey)
	builtKey := string(builder.Build())

	// Grant ACL using our consensus helper with permission
	err := consensus.GrantACLToKeyWithPermission(ctx, metaStore, builtKey, principal, permissions)
	if err != nil {
		options.Logger.Error("Failed to grant ACL",
			zap.String("principal", principal),
			zap.String("tableKey", tableKey),
			zap.String("permissions", permissions),
			zap.Error(err))
		return nil, fmt.Errorf("failed to grant ACL: %w", err)
	}

	options.Logger.Info("ACL granted successfully",
		zap.String("principal", principal),
		zap.String("tableKey", tableKey),
		zap.String("permissions", permissions))

	return fmt.Appendf(nil, "ACL granted to %s for %s with permissions %s", principal, builtKey, permissions), nil
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

	// Get the metadata store
	kvPool := kv.GetPool()
	if kvPool == nil {
		return nil, fmt.Errorf("KV pool not initialized")
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}

	// Convert dotted key to KeyBuilder format to match what ReadKey uses
	builder := kv.FromDottedKey(tableKey)
	builtKey := string(builder.Build())

	// Revoke ACL using our consensus helper with permission
	err := consensus.RevokeACLFromKeyWithPermission(ctx, metaStore, builtKey, principal, permissions)
	if err != nil {
		options.Logger.Error("Failed to revoke ACL",
			zap.String("principal", principal),
			zap.String("tableKey", tableKey),
			zap.String("permissions", permissions),
			zap.Error(err))
		return nil, fmt.Errorf("failed to revoke ACL: %w", err)
	}

	options.Logger.Info("ACL revoked successfully",
		zap.String("principal", principal),
		zap.String("tableKey", tableKey),
		zap.String("permissions", permissions))

	return fmt.Appendf(nil, "ACL revoked from %s for %s with permissions %s", principal, builtKey, permissions), nil
}
