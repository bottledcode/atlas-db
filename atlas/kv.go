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

package atlas

import (
	"context"
)

func WriteKey(ctx context.Context, key []byte, value []byte) error {
	return nil
}

func AddOwner(ctx context.Context, key []byte, owner string) error {
	return nil
}

func RevokeOwner(ctx context.Context, key []byte, owner string) error {
	return nil
}

func AddWriter(ctx context.Context, key []byte, writer string) error {
	return nil
}

func RevokeWriter(ctx context.Context, key []byte, writer string) error {
	return nil
}

func AddReader(ctx context.Context, key []byte, reader string) error {
	return nil
}

func RevokeReader(ctx context.Context, key []byte, reader string) error {
	return nil
}

func GetKey(ctx context.Context, key []byte) ([]byte, error) {
	return nil, nil
}

// PrefixScan performs a distributed prefix scan across all nodes in the cluster.
// It returns all keys matching the prefix that are owned by any node.
func PrefixScan(ctx context.Context, keyPrefix []byte) ([]string, error) {
	return nil, nil
}
