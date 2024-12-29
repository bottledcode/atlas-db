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
