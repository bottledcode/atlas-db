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
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
)

type KeyCommand struct {
	CommandString
}

func (c *KeyCommand) GetKey() *KeyGetCommand {
	if p, ok := c.SelectNormalizedCommand(1); ok && p == "GET" {
		return &KeyGetCommand{*c}
	}
	return nil
}

func (c *KeyCommand) GetNext() (Command, error) {
	if next, ok := c.SelectNormalizedCommand(1); ok {
		switch next {
		case "GET":
			return &KeyGetCommand{*c}, nil
		case "PUT":
			fallthrough
		case "SET":
			return &KeyPutCommand{*c}, nil
		case "DEL":
			return &KeyDelCommand{*c}, nil
		case "BLOB":
			if blobOp, ok := c.SelectNormalizedCommand(2); ok {
				switch blobOp {
				case "SET":
					return &KeyBlobSetCommand{*c}, nil
				case "GET":
					return &KeyBlobGetCommand{*c}, nil
				}
			}
		}
	}
	return EmptyCommandString, nil
}

func (c *KeyCommand) FromKey(key string) *kv.KeyBuilder {
	return kv.FromDottedKey(key)
}

type KeyPutCommand struct {
	KeyCommand
}

func (k *KeyPutCommand) GetNext() (Command, error) {
	return k, nil
}

// KeyPutParsed holds the parsed components of a KEY PUT command.
type KeyPutParsed struct {
	Key       string
	Principal string
	WormTTL   *time.Duration
	WormExp   *time.Time
	Value     []byte
}

// Parse extracts arguments for KEY PUT without performing any side effects.
// It intentionally treats a bare "WORM" token followed by no TTL/EXPIRES as part of the value.
func (k *KeyPutCommand) Parse() (*KeyPutParsed, error) {
	if err := k.CheckMinLen(4); err != nil {
		return nil, err
	}
	key, _ := k.SelectNormalizedCommand(2)
	var principal string
	var wormTtl *time.Duration
	var wormExp *time.Time
	var value []byte

	nextChunk := 3
	for {
		// If there are no more tokens, value must be empty string
		if nextChunk >= k.NormalizedLen() {
			value = []byte("")
			break
		}

		next, _ := k.SelectNormalizedCommand(nextChunk)
		switch next {
		case "PRINCIPAL":
			// Need name and value after PRINCIPAL
			if nextChunk+1 >= k.NormalizedLen() {
				// Not enough tokens to parse principal; treat as value
				value = []byte(k.From(nextChunk).raw)
				nextChunk = k.NormalizedLen()
				break
			}
			principal = k.SelectCommand(nextChunk + 1)
			nextChunk += 2
		case "WORM":
			// WORM must be followed by TTL <dur> or EXPIRES <ts>; otherwise it's value text
			if nextChunk+2 >= k.NormalizedLen() {
				// Not enough tokens to form a WORM attribute; treat as value
				value = []byte(k.From(nextChunk).raw)
				nextChunk = k.NormalizedLen()
				break
			}
			t, _ := k.SelectNormalizedCommand(nextChunk + 1)
			switch t {
			case "TTL":
				durStr := k.SelectCommand(nextChunk + 2)
				dur, err := time.ParseDuration(durStr)
				if err != nil {
					return nil, err
				}
				wormTtl = &dur
				nextChunk += 3
			case "EXPIRES":
				tsStr := k.SelectCommand(nextChunk + 2)
				ts, err := time.Parse(time.RFC3339, tsStr)
				if err != nil {
					return nil, err
				}
				wormExp = &ts
				nextChunk += 3
			default:
				// Unknown sub-option after WORM, treat as value
				value = []byte(k.From(nextChunk).raw)
				nextChunk = k.NormalizedLen()
			}
		default:
			value = []byte(k.From(nextChunk).raw)
			nextChunk = k.NormalizedLen()
		}

		if nextChunk >= k.NormalizedLen() && value == nil {
			// No explicit value provided; default to empty
			value = []byte("")
		}
		if nextChunk >= k.NormalizedLen() && value != nil {
			break
		}
	}

	return &KeyPutParsed{
		Key:       key,
		Principal: principal,
		WormTTL:   wormTtl,
		WormExp:   wormExp,
		Value:     value,
	}, nil
}

func (k *KeyPutCommand) Execute(ctx context.Context) ([]byte, error) {
	parsed, err := k.Parse()
	if err != nil {
		return nil, err
	}
	builder := k.FromKey(parsed.Key)
	if err := atlas.WriteKey(ctx, builder, parsed.Value); err != nil {
		return nil, err
	}
	// Log attributes for observability
	var ttl time.Duration
	if parsed.WormTTL != nil {
		ttl = *parsed.WormTTL
	}
	var exp time.Time
	if parsed.WormExp != nil {
		exp = *parsed.WormExp
	}
	options.Logger.Info("wrote key", zap.String("key", parsed.Key), zap.String("principal", parsed.Principal), zap.Duration("wormTtl", ttl), zap.Time("wormExp", exp))
	return nil, nil
}

type KeyGetCommand struct {
	KeyCommand
}

func (k *KeyGetCommand) GetNext() (Command, error) {
	return k, nil
}

func (k *KeyGetCommand) Execute(ctx context.Context) ([]byte, error) {
	if err := k.CheckMinLen(3); err != nil {
		return nil, err
	}
	key, _ := k.SelectNormalizedCommand(2)

	builder := k.FromKey(key)
	value, err := atlas.GetKey(ctx, builder)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return []byte("NOT_FOUND"), nil
	}
	resp := bytes.Join([][]byte{[]byte("VALUE:"), value}, nil)
	return resp, nil
}

type KeyDelCommand struct {
	KeyCommand
}

func (k *KeyDelCommand) GetNext() (Command, error) {
	return k, nil
}

func (k *KeyDelCommand) Execute(ctx context.Context) ([]byte, error) {
	if err := k.CheckMinLen(3); err != nil {
		return nil, err
	}
	key, _ := k.SelectNormalizedCommand(2)
	builder := k.FromKey(key)
	if err := atlas.DeleteKey(ctx, builder); err != nil {
		return nil, err
	}
	return nil, nil
}

type KeyBlobSetCommand struct {
	KeyCommand
}

func (k *KeyBlobSetCommand) GetNext() (Command, error) {
	return k, nil
}

func (k *KeyBlobSetCommand) Execute(ctx context.Context) ([]byte, error) {
	if err := k.CheckMinLen(5); err != nil {
		return nil, err
	}
	key, _ := k.SelectNormalizedCommand(3)
	binaryData := k.GetBinaryData()
	if binaryData == nil {
		return nil, errors.New("binary data not attached to command")
	}

	builder := k.FromKey(key)
	if err := atlas.WriteKey(ctx, builder, binaryData); err != nil {
		return nil, err
	}

	options.Logger.Info("wrote blob", zap.String("key", key), zap.Int("size", len(binaryData)))
	return nil, nil
}

type KeyBlobGetCommand struct {
	KeyCommand
}

func (k *KeyBlobGetCommand) GetNext() (Command, error) {
	return k, nil
}

func (k *KeyBlobGetCommand) Execute(ctx context.Context) ([]byte, error) {
	if err := k.CheckMinLen(4); err != nil {
		return nil, err
	}
	key, _ := k.SelectNormalizedCommand(3)

	builder := k.FromKey(key)
	value, err := atlas.GetKey(ctx, builder)
	if err != nil {
		return nil, err
	}

	return value, nil
}
