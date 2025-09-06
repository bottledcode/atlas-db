#!/bin/bash

#
# This file is part of Atlas-DB.
#
# Atlas-DB is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
#
# Atlas-DB is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Atlas-DB. If not, see <https://www.gnu.org/licenses/>.
#
#

# End-to-End ACL Testing Script
# Tests ACL GRANT/REVOKE commands in a real cluster setup

set -e

echo "🧪 Starting Atlas-DB ACL End-to-End Test"
echo "========================================"

# Cleanup function
cleanup() {
    echo "🧹 Cleaning up..."
    if [ ! -z "$CADDY_PID" ] && ps -p $CADDY_PID > /dev/null 2>&1; then
        echo "Stopping Caddy (PID: $CADDY_PID)"
        kill $CADDY_PID
        wait $CADDY_PID 2>/dev/null || true
    fi
    
    # Clean up database files
    rm -rf /tmp/atlas2/
    echo "✅ Cleanup complete"
}

# Set trap for cleanup
trap cleanup EXIT

echo "📂 Setting up test environment..."
mkdir -p /tmp/atlas2

echo "🚀 Starting Caddy with Atlas-DB..."
./caddy run --config atlas/caddy/Caddyfile2 &
CADDY_PID=$!

echo "⏳ Waiting for Atlas-DB to start..."
sleep 3

# Check if socket exists
if [ ! -S "/tmp/atlas2/socket" ]; then
    echo "❌ Socket not found at /tmp/atlas2/socket"
    echo "Caddy might not have started properly. Checking processes:"
    ps aux | grep caddy
    exit 1
fi

echo "✅ Atlas-DB cluster is running"

echo "🔍 Testing ACL commands..."

# Function to run a command via the REPL
run_command() {
    local cmd="$1"
    echo "  → Running: $cmd"
    echo "$cmd" | timeout 5s ./caddy atlas /tmp/atlas2/socket 2>&1 | sed '/^Error: EOF$/d' || {
        #echo "❌ Command failed or timed out: $cmd"
        return 0
    }
}

# Function to run multiple commands in a single session
run_session_commands() {
    local commands=("$@")
    echo "  → Running session with commands: ${commands[*]}"
    (
        for cmd in "${commands[@]}"; do
            echo "$cmd"
        done
    ) | timeout 10s ./caddy atlas /tmp/atlas2/socket 2>&1 | sed '/^Error: EOF$/d' || {
        return 0
    }
}

echo "📝 1. Setting up test data..."
run_command "KEY PUT users.alice hello"
run_command "KEY PUT users.bob world" 

echo "🔓 2. Testing public access (should work)..."
result=$(run_command "KEY GET users.alice")
if echo "$result" | grep -q "VALUE:hello"; then
    echo "✅ Public read successful"
else
    echo "❌ Public read failed: $result"
    exit 1
fi

echo "🔒 3. Granting ACL to restrict access..."
run_command "ACL GRANT users.alice alice PERMS READ"

echo "🚫 4. Testing restricted access without principal (should fail)..."
result=$(run_command "KEY GET users.alice")
if echo "$result" | grep -q "NOT_FOUND"; then
    echo "✅ Access correctly denied without principal (returns NOT_FOUND for security)"
else
    echo "❌ Expected NOT_FOUND, got: $result"
    exit 1
fi

echo "🔐 5. Testing access with correct principal (should work)..."
result=$(run_session_commands "PRINCIPAL ASSUME alice" "KEY GET users.alice")
if echo "$result" | grep -q "VALUE:hello"; then
    echo "✅ Access granted to correct principal"
else
    echo "❌ Expected access granted to alice, got: $result"
    echo "  Debugging session state..."
    debug_result=$(run_session_commands "PRINCIPAL ASSUME alice" "PRINCIPAL WHOAMI" "KEY GET users.alice")
    echo "  Debug result: $debug_result"
    exit 1
fi

echo "🚫 6. Testing access with wrong principal (should fail)..."
result=$(run_session_commands "PRINCIPAL ASSUME bob" "KEY GET users.alice")
if echo "$result" | grep -q "NOT_FOUND"; then
    echo "✅ Access correctly denied to wrong principal (returns NOT_FOUND for security)"
else
    echo "❌ Expected NOT_FOUND for bob, got: $result"
    exit 1
fi

echo "🔄 7. Testing ACL REVOKE..."
run_command "ACL REVOKE users.alice alice PERMS READ"

echo "🔓 8. Testing access after revoke (should become public again)..."
result=$(run_command "KEY GET users.alice")
if echo "$result" | grep -q "VALUE:hello"; then
    echo "✅ Access reverted to public after ACL removal"
else
    echo "❌ Expected public access after revoke, got: $result"
    exit 1
fi

echo "📊 9. Testing multiple principals and permissions..."
run_command "ACL GRANT users.bob alice PERMS READ"
run_command "ACL GRANT users.bob bob PERMS WRITE"

echo "🔐 10. Testing read access with permissions..."
result=$(run_session_commands "PRINCIPAL ASSUME alice" "KEY GET users.bob")
if echo "$result" | grep -q "VALUE:world"; then
    echo "✅ Alice can read users.bob"
else
    echo "❌ Alice should be able to read users.bob, got: $result"
    exit 1
fi

result=$(run_session_commands "PRINCIPAL ASSUME bob" "KEY GET users.bob")
if echo "$result" | grep -q "NOT_FOUND"; then
    echo "✅ Bob cannot read users.bob without READ permission (as expected)"
else
    echo "❌ Bob should NOT be able to read users.bob without READ; got: $result"
    exit 1
fi

echo "✍️ 11. Testing write access enforcement..."
# Bob has WRITE on users.bob; verify write succeeds for bob
result=$(run_session_commands "PRINCIPAL ASSUME bob" "KEY PUT users.bob updated_by_bob")
if echo "$result" | grep -qi "ERROR"; then
    echo "❌ Bob should be able to WRITE users.bob, got error: $result"
    exit 1
else
    echo "✅ Bob can WRITE users.bob"
fi

# Alice lacks WRITE; verify write denied
result=$(run_session_commands "PRINCIPAL ASSUME alice" "KEY PUT users.bob updated_by_alice")
if echo "$result" | grep -qi "permission denied"; then
    echo "✅ Alice write correctly denied"
else
    echo "❌ Alice write should be denied, got: $result"
    exit 1
fi

# No principal; verify write denied when WRITE ACL exists
result=$(run_command "KEY PUT users.bob anonymous_write")
if echo "$result" | grep -qi "permission denied"; then
    echo "✅ Anonymous write correctly denied when WRITE ACL exists"
else
    echo "❌ Anonymous write should be denied when WRITE ACL exists, got: $result"
    exit 1
fi

echo "🔄 12. Testing revoke from multiple principals..."
run_command "ACL REVOKE users.bob alice PERMS READ"

result=$(run_session_commands "PRINCIPAL ASSUME alice" "KEY GET users.bob")
if echo "$result" | grep -q "NOT_FOUND"; then
    echo "✅ Alice correctly denied after revoke (returns NOT_FOUND for security)"
else
    echo "❌ Alice should be denied after revoke, got: $result"
    exit 1
fi

result=$(run_session_commands "PRINCIPAL ASSUME bob" "KEY GET users.bob")
if echo "$result" | grep -q "VALUE:"; then
    echo "✅ Bob still has access after alice revoke"
else
    echo "❌ Bob should still have access, got: $result"
    exit 1
fi

echo "✨ All ACL command tests completed successfully!"
echo ""
echo "📋 Test Summary:"
echo "  ✅ ACL GRANT with key-level access"
echo "  ✅ ACL REVOKE operations"
echo "  ✅ Multiple principal management"
echo "  ✅ Command parsing and execution"
echo "  ✅ Permission enforcement without principal"
echo "  ✅ Permission enforcement with correct principal"
echo "  ✅ Permission enforcement with wrong principal" 
echo "  ✅ Access revocation verification"
echo "  ✅ Multi-principal access control"
echo "  ✅ Selective revocation from multiple principals"
echo "  ✅ Separate READ and WRITE permissions enforced"
echo ""
echo "🎉 End-to-End ACL Test: PASSED"
