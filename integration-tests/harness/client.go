//go:build integration
// +build integration

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

package harness

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

type SocketClient struct {
	socketPath string
	conn       net.Conn
}

func NewSocketClient(socketPath string) *SocketClient {
	return &SocketClient{
		socketPath: socketPath,
	}
}

func (sc *SocketClient) Connect() error {
	var err error
	for i := 0; i < 30; i++ {
		sc.conn, err = net.Dial("unix", sc.socketPath)
		if err == nil {
			if err := sc.performHandshake(); err != nil {
				_ = sc.conn.Close()
				sc.conn = nil
				return fmt.Errorf("handshake failed: %w", err)
			}
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("failed to connect to socket %s after retries: %w", sc.socketPath, err)
}

func (sc *SocketClient) performHandshake() error {
	reader := bufio.NewReader(sc.conn)

	welcome, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read welcome: %w", err)
	}

	if !strings.HasPrefix(welcome, "WELCOME") {
		return fmt.Errorf("unexpected welcome message: %s", welcome)
	}

	_, err = sc.conn.Write([]byte("HELLO 1.0 ClientID=integration-test\r\n"))
	if err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	ready, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read ready: %w", err)
	}

	if !strings.HasPrefix(ready, "READY") {
		return fmt.Errorf("unexpected ready message: %s", ready)
	}

	return nil
}

func (sc *SocketClient) Close() error {
	if sc.conn != nil {
		err := sc.conn.Close()
		sc.conn = nil
		return err
	}
	return nil
}

func (sc *SocketClient) ExecuteCommand(cmd string) (string, error) {
	if sc.conn == nil {
		if err := sc.Connect(); err != nil {
			return "", err
		}
	}

	_, err := sc.conn.Write([]byte(cmd + "\n"))
	if err != nil {
		if sc.conn != nil {
			sc.conn.Close()
			sc.conn = nil
		}
		return "", fmt.Errorf("write command: %w", err)
	}

	reader := bufio.NewReader(sc.conn)
	var response strings.Builder

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if sc.conn != nil {
				sc.conn.Close()
				sc.conn = nil
			}
			return "", fmt.Errorf("read response: %w", err)
		}

		response.WriteString(line)

		if strings.Contains(line, "OK") ||
		   strings.Contains(line, "ERROR") ||
		   strings.Contains(line, "VALUE:") ||
		   strings.Contains(line, "NOT_FOUND") ||
		   strings.Contains(line, "EMPTY") ||
		   strings.Contains(line, "permission denied") {
			break
		}

		// For KEYS: response, parse count and read exact number of key lines, then expect OK
		if strings.HasPrefix(line, "KEYS:") {
			// Parse the count from "KEYS:<count>"
			var count int
			_, err := fmt.Sscanf(line, "KEYS:%d", &count)
			if err == nil {
				// Read exactly 'count' key lines
				for i := 0; i < count; i++ {
					nextLine, err := reader.ReadString('\n')
					if err != nil {
						break
					}
					response.WriteString(nextLine)
				}
				// Read the final OK line
				okLine, err := reader.ReadString('\n')
				if err == nil {
					response.WriteString(okLine)
				}
			}
			break
		}
	}

	return strings.TrimSpace(response.String()), nil
}

func (sc *SocketClient) ExecuteSession(commands []string) ([]string, error) {
	if sc.conn == nil {
		if err := sc.Connect(); err != nil {
			return nil, err
		}
	}

	responses := make([]string, 0, len(commands))

	for _, cmd := range commands {
		resp, err := sc.ExecuteCommand(cmd)
		if err != nil {
			return responses, err
		}
		responses = append(responses, resp)
	}

	return responses, nil
}

func (sc *SocketClient) KeyGet(key string) (string, error) {
	resp, err := sc.ExecuteCommand(fmt.Sprintf("KEY GET %s", key))
	if err != nil {
		return "", err
	}

	if strings.Contains(resp, "VALUE:") {
		parts := strings.SplitN(resp, "VALUE:", 2)
		if len(parts) == 2 {
			return strings.TrimSpace(parts[1]), nil
		}
	}

	return "", fmt.Errorf("unexpected response: %s", resp)
}

func (sc *SocketClient) KeyPut(key, value string) error {
	resp, err := sc.ExecuteCommand(fmt.Sprintf("KEY PUT %s %s", key, value))
	if err != nil {
		return err
	}

	if !strings.Contains(resp, "OK") {
		return fmt.Errorf("unexpected response: %s", resp)
	}

	return nil
}

func (sc *SocketClient) Scan(prefix string) ([]string, error) {
	resp, err := sc.ExecuteCommand(fmt.Sprintf("SCAN %s", prefix))
	if err != nil {
		return nil, err
	}

	if strings.HasPrefix(resp, "EMPTY") {
		return []string{}, nil
	}

	// Parse response: KEYS:<count>\n<key1>\n<key2>\n...
	lines := strings.Split(resp, "\n")

	if len(lines) < 1 || !strings.HasPrefix(lines[0], "KEYS:") {
		return nil, fmt.Errorf("unexpected response format: %s", resp)
	}

	// Skip the first line (KEYS:<count>) and return the rest
	if len(lines) == 1 {
		return []string{}, nil
	}

	// Filter out empty lines and the OK terminator
	keys := make([]string, 0, len(lines)-1)
	for i := 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed != "" && trimmed != "OK" {
			keys = append(keys, trimmed)
		}
	}

	return keys, nil
}

func (sc *SocketClient) AssumeIdentity(principal string) error {
	resp, err := sc.ExecuteCommand(fmt.Sprintf("PRINCIPAL ASSUME %s", principal))
	if err != nil {
		return err
	}

	if !strings.Contains(resp, "OK") {
		return fmt.Errorf("unexpected response: %s", resp)
	}

	return nil
}

func (sc *SocketClient) GrantACL(key, principal, perms string) error {
	resp, err := sc.ExecuteCommand(fmt.Sprintf("ACL GRANT %s %s PERMS %s", key, principal, perms))
	if err != nil {
		return err
	}

	if !strings.Contains(resp, "OK") {
		return fmt.Errorf("unexpected response: %s", resp)
	}

	return nil
}

func (sc *SocketClient) RevokeACL(key, principal, perms string) error {
	resp, err := sc.ExecuteCommand(fmt.Sprintf("ACL REVOKE %s %s PERMS %s", key, principal, perms))
	if err != nil {
		return err
	}

	if !strings.Contains(resp, "OK") {
		return fmt.Errorf("unexpected response: %s", resp)
	}

	return nil
}

func (sc *SocketClient) WaitForValue(key string, expectedValue string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		val, err := sc.KeyGet(key)
		if err == nil && val == expectedValue {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for key %s to have value %s", key, expectedValue)
}
