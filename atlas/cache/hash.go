package cache

import (
	"unsafe"

	"github.com/zeebo/xxh3"
)

func hashKey[K Key](key K) uint64 {
	return xxh3.Hash(keyToBytes(key))
}

func keyToBytes[K Key](key K) []byte {
	switch k := any(key).(type) {
	case []byte:
		return k
	case string:
		return unsafe.Slice(unsafe.StringData(k), len(k))
	default:
		// For ~string types, convert via string
		s := string(key)
		return unsafe.Slice(unsafe.StringData(s), len(s))
	}
}
