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

package test

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func GetTempDb(t *testing.T) (string, func()) {
	f, err := os.CreateTemp("", "initialize-maybe*")
	require.NoError(t, err)
	f.Close()
	return f.Name(), func() {
		os.Remove(f.Name())
		os.Remove(f.Name() + "-wal")
		os.Remove(f.Name() + "-shm")
	}
}
