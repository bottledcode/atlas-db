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

package socket

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/atlas/commands"
)

func TestBlobScanner_DetectsBlobSetCommand(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "Valid BLOB SET command",
			input:    "KEY BLOB SET mykey 10",
			expected: true,
		},
		{
			name:     "Valid blob set with lowercase",
			input:    "key blob set mykey 100",
			expected: true,
		},
		{
			name:     "Invalid - too few arguments",
			input:    "KEY BLOB SET",
			expected: false,
		},
		{
			name:     "Invalid - different command",
			input:    "KEY GET mykey",
			expected: false,
		},
		{
			name:     "Invalid - BLOB GET not SET",
			input:    "KEY BLOB GET mykey",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := commands.CommandFromString(tt.input)
			result := isBlobSetCommand(cmd)
			if result != tt.expected {
				t.Errorf("isBlobSetCommand(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestBlobScanner_ReadBinaryDataWithNewlines(t *testing.T) {
	tests := []struct {
		name        string
		command     string
		binaryData  []byte
		expectError bool
	}{
		{
			name:        "Binary data with embedded newlines",
			command:     "KEY BLOB SET mykey 10\r\n",
			binaryData:  []byte("test\r\ndata"),
			expectError: false,
		},
		{
			name:        "Binary data with multiple newlines",
			command:     "KEY BLOB SET mykey 15\r\n",
			binaryData:  []byte("line1\r\nline2\r\n3"),
			expectError: false,
		},
		{
			name:        "Binary data with null bytes",
			command:     "KEY BLOB SET mykey 8\r\n",
			binaryData:  []byte{0x00, 0x01, 0x02, 0x0d, 0x0a, 0x03, 0x04, 0x05},
			expectError: false,
		},
		{
			name:        "Empty binary data",
			command:     "KEY BLOB SET mykey 0\r\n",
			binaryData:  []byte{},
			expectError: false,
		},
		{
			name:        "Binary data with only newlines",
			command:     "KEY BLOB SET mykey 4\r\n",
			binaryData:  []byte("\r\n\r\n"),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			buf.WriteString(tt.command)
			buf.Write(tt.binaryData)

			reader := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
			scanner := NewScanner(reader)

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			cmds, errs := scanner.Scan(ctx)

			select {
			case cmd := <-cmds:
				if cmd == nil {
					t.Fatal("expected command, got nil")
				}

				binData := cmd.GetBinaryData()
				if !bytes.Equal(binData, tt.binaryData) {
					t.Errorf("binary data mismatch:\ngot:  %v\nwant: %v", binData, tt.binaryData)
				}

				key, _ := cmd.SelectNormalizedCommand(3)
				if key != "MYKEY" {
					t.Errorf("key = %q, want %q", key, "MYKEY")
				}

			case err := <-errs:
				if !tt.expectError {
					t.Fatalf("unexpected error: %v", err)
				}

			case <-ctx.Done():
				t.Fatal("timeout waiting for command")
			}
		})
	}
}

func TestBlobScanner_InvalidLength(t *testing.T) {
	tests := []struct {
		name    string
		command string
	}{
		{
			name:    "Negative length",
			command: "KEY BLOB SET mykey -1\r\n",
		},
		{
			name:    "Invalid length string",
			command: "KEY BLOB SET mykey abc\r\n",
		},
		{
			name:    "Length exceeds maximum",
			command: "KEY BLOB SET mykey 999999999999\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			buf.WriteString(tt.command)

			reader := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
			scanner := NewScanner(reader)

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			cmds, errs := scanner.Scan(ctx)

			select {
			case <-cmds:
				t.Fatal("expected error, got command")
			case err := <-errs:
				if err == nil {
					t.Fatal("expected error, got nil")
				}
			case <-ctx.Done():
				t.Fatal("timeout waiting for error")
			}
		})
	}
}

func TestBlobGetCommand_Detection(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "Valid BLOB GET command",
			input:    "KEY BLOB GET mykey",
			expected: true,
		},
		{
			name:     "Valid blob get with lowercase",
			input:    "key blob get mykey",
			expected: true,
		},
		{
			name:     "Invalid - BLOB SET not GET",
			input:    "KEY BLOB SET mykey 10",
			expected: false,
		},
		{
			name:     "Invalid - regular GET",
			input:    "KEY GET mykey",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := commands.CommandFromString(tt.input)
			result := isBlobGetCommand(cmd)
			if result != tt.expected {
				t.Errorf("isBlobGetCommand(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestWriteBlobResponse_NilVsEmpty(t *testing.T) {
	tests := []struct {
		name           string
		binaryData     []byte
		expectedOutput string
	}{
		{
			name:           "Nil data returns EMPTY",
			binaryData:     nil,
			expectedOutput: "EMPTY\r\n",
		},
		{
			name:           "Empty slice returns BLOB 0",
			binaryData:     []byte{},
			expectedOutput: "BLOB 0\r\n",
		},
		{
			name:           "Non-empty data returns BLOB with length",
			binaryData:     []byte{0x01, 0x02, 0x03},
			expectedOutput: "BLOB 3\r\n\x01\x02\x03",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writeBuf bytes.Buffer
			var readBuf bytes.Buffer

			reader := bufio.NewReader(&readBuf)
			writer := bufio.NewWriter(&writeBuf)
			rw := bufio.NewReadWriter(reader, writer)

			socket := &Socket{
				writer:  rw,
				conn:    &mockConn{},
				timeout: 1 * time.Second,
			}

			err := socket.writeBlobResponse(tt.binaryData)
			if err != nil {
				t.Fatalf("writeBlobResponse failed: %v", err)
			}

			output := writeBuf.String()
			if output != tt.expectedOutput {
				t.Errorf("output mismatch:\ngot:  %q\nwant: %q", output, tt.expectedOutput)
			}
		})
	}
}

// mockConn implements net.Conn for testing
type mockConn struct{}

func (m *mockConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (m *mockConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (m *mockConn) Close() error                       { return nil }
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }
