#!/bin/bash

# Atlas DB test runner script

set -e

echo "Running Atlas DB test suite..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_test() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Go is available
if ! command -v go &> /dev/null; then
    print_error "Go is not installed or not in PATH"
    exit 1
fi

print_status "Go version: $(go version)"

# Change to project directory
cd "$(dirname "$0")/.."

# Build first to catch compilation errors
print_test "Building project..."
if ! go build ./...; then
    print_error "Build failed - fixing compilation errors first"
    exit 1
fi

print_status "Build successful"

# Function to run tests for a specific package
run_package_tests() {
    local package=$1
    local name=$2
    
    print_test "Running $name tests..."
    
    if go test -v "./$package" -timeout=30s; then
        print_status "$name tests passed ✓"
        return 0
    else
        print_error "$name tests failed ✗"
        return 1
    fi
}

# Function to run tests with race detection
run_race_tests() {
    local package=$1
    local name=$2
    
    print_test "Running $name tests with race detection..."
    
    if go test -race -v "./$package" -timeout=60s; then
        print_status "$name race tests passed ✓"
        return 0
    else
        print_error "$name race tests failed ✗"
        return 1
    fi
}

# Function to run benchmarks
run_benchmarks() {
    local package=$1
    local name=$2
    
    print_test "Running $name benchmarks..."
    
    if go test -bench=. -benchmem "./$package" -timeout=120s; then
        print_status "$name benchmarks completed ✓"
        return 0
    else
        print_warning "$name benchmarks had issues (continuing...)"
        return 0
    fi
}

# Track test results
TOTAL_TESTS=0
PASSED_TESTS=0

# Test packages in order
PACKAGES=(
    "pkg/config:Configuration"
    "pkg/storage:Storage Layer"
    "pkg/consensus:Consensus Algorithm"
    "pkg/client:Client Library"
    "internal/server:Server Integration"
)

print_status "Running unit tests..."

for package_info in "${PACKAGES[@]}"; do
    IFS=':' read -r package name <<< "$package_info"
    
    if [ -f "$package"/*_test.go ]; then
        TOTAL_TESTS=$((TOTAL_TESTS + 1))
        if run_package_tests "$package" "$name"; then
            PASSED_TESTS=$((PASSED_TESTS + 1))
        fi
    else
        print_warning "No tests found for $name ($package)"
    fi
done

echo ""
print_status "Unit test summary: $PASSED_TESTS/$TOTAL_TESTS packages passed"

# Run race detection tests if requested
if [ "$1" = "--race" ] || [ "$1" = "-r" ]; then
    echo ""
    print_status "Running race detection tests..."
    
    for package_info in "${PACKAGES[@]}"; do
        IFS=':' read -r package name <<< "$package_info"
        
        if [ -f "$package"/*_test.go ]; then
            run_race_tests "$package" "$name"
        fi
    done
fi

# Run benchmarks if requested
if [ "$1" = "--bench" ] || [ "$1" = "-b" ]; then
    echo ""
    print_status "Running benchmarks..."
    
    for package_info in "${PACKAGES[@]}"; do
        IFS=':' read -r package name <<< "$package_info"
        
        if [ -f "$package"/*_test.go ]; then
            run_benchmarks "$package" "$name"
        fi
    done
fi

# Run coverage if requested
if [ "$1" = "--coverage" ] || [ "$1" = "-c" ]; then
    echo ""
    print_test "Running test coverage analysis..."
    
    if go test -coverprofile=coverage.out ./...; then
        print_status "Generating coverage report..."
        go tool cover -html=coverage.out -o coverage.html
        print_status "Coverage report generated: coverage.html"
        
        # Show coverage summary
        print_status "Coverage summary:"
        go tool cover -func=coverage.out | tail -1
    else
        print_error "Coverage analysis failed"
    fi
fi

# Run all tests with verbose output if requested
if [ "$1" = "--all" ] || [ "$1" = "-a" ]; then
    echo ""
    print_test "Running complete test suite..."
    
    if go test -v ./... -timeout=120s; then
        print_status "Complete test suite passed ✓"
    else
        print_error "Some tests in complete suite failed ✗"
        exit 1
    fi
fi

echo ""
if [ $PASSED_TESTS -eq $TOTAL_TESTS ]; then
    print_status "🎉 All tests passed! Atlas DB is ready."
    exit 0
else
    print_error "❌ $((TOTAL_TESTS - PASSED_TESTS)) test package(s) failed."
    exit 1
fi