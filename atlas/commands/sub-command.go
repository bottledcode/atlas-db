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
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
)

type SubCommand struct {
	CommandString
}

func (c *SubCommand) GetNext() (Command, error) {
	return c, nil
}

// SubParsed holds the parsed components of a SUB command.
type SubParsed struct {
	Prefix         string
	URL            string
	Batch          bool
	RetryAttempts  int32
	RetryAfterBase time.Duration
	Auth           string
}

// Parse extracts arguments for SUB command.
// SUB <prefix> <url> [BATCH] [RETRY <attempts>] [RETRY_AFTER <duration>] [AUTH <token>]
func (c *SubCommand) Parse() (*SubParsed, error) {
	if err := c.CheckMinLen(3); err != nil {
		return nil, err
	}

	prefix, _ := c.SelectNormalizedCommand(1)
	url := c.SelectCommand(2) // Use raw version to preserve case for URL

	parsed := &SubParsed{
		Prefix:         prefix,
		URL:            url,
		Batch:          false, // Default to non-batched
		RetryAttempts:  3,     // Default 3 retries
		RetryAfterBase: 100 * time.Millisecond,
	}

	// Parse optional flags
	for i := 3; i < c.NormalizedLen(); i++ {
		flag, _ := c.SelectNormalizedCommand(i)
		switch flag {
		case "BATCH":
			parsed.Batch = true
		case "RETRY":
			// Need attempts after RETRY
			if i+1 >= c.NormalizedLen() {
				return nil, errors.New("RETRY requires attempts number")
			}
			attemptsStr := c.SelectCommand(i + 1)
			attempts, err := strconv.ParseInt(attemptsStr, 10, 32)
			if err != nil {
				return nil, errors.New("RETRY requires valid number")
			}
			parsed.RetryAttempts = int32(attempts)
			i++ // Skip next argument
		case "RETRY_AFTER":
			// Need duration after RETRY_AFTER
			if i+1 >= c.NormalizedLen() {
				return nil, errors.New("RETRY_AFTER requires duration")
			}
			durationStr := c.SelectCommand(i + 1)
			dur, err := time.ParseDuration(durationStr)
			if err != nil {
				return nil, err
			}
			parsed.RetryAfterBase = dur
			i++ // Skip next argument
		case "AUTH":
			// Need token after AUTH
			if i+1 >= c.NormalizedLen() {
				return nil, errors.New("AUTH requires token")
			}
			parsed.Auth = c.SelectCommand(i + 1)
			i++ // Skip next argument
		}
	}

	return parsed, nil
}

func (c *SubCommand) Execute(ctx context.Context) ([]byte, error) {
	parsed, err := c.Parse()
	if err != nil {
		return nil, err
	}

	// Create subscription protobuf
	sub := &consensus.Subscribe{
		Url:    parsed.URL,
		Prefix: []byte(parsed.Prefix),
		Options: &consensus.SubscribeOptions{
			Batch:          parsed.Batch,
			RetryAttempts:  parsed.RetryAttempts,
			RetryAfterBase: durationpb.New(parsed.RetryAfterBase),
			Auth:           parsed.Auth,
		},
	}

	// Write subscription to the user's prefix key
	// The consensus layer will handle routing this to the appropriate magic key
	qm := consensus.GetDefaultQuorumManager(ctx)
	q, err := qm.GetQuorum(ctx, consensus.KeyName(parsed.Prefix))
	if err != nil {
		options.Logger.Error("failed to get quorum for subscription",
			zap.Error(err),
			zap.String("prefix", parsed.Prefix))
		return nil, fmt.Errorf("failed to get quorum for subscription: %w", err)
	}

	// Write subscription to the quorum
	resp, err := q.WriteKey(ctx, &consensus.WriteKeyRequest{
		Sender: nil,
		Table:  consensus.KeyName(parsed.Prefix),
		Value: &consensus.KVChange{
			Operation: &consensus.KVChange_Sub{
				Sub: sub,
			},
		},
	})
	if err != nil {
		options.Logger.Error("failed to write subscription to quorum",
			zap.Error(err),
			zap.String("prefix", parsed.Prefix),
			zap.String("url", parsed.URL))
		return nil, fmt.Errorf("failed to write subscription to quorum: %w", err)
	}
	if resp.Error != "" {
		options.Logger.Error("failed to write subscription from quorum",
			zap.Error(errors.New(resp.Error)),
			zap.String("prefix", parsed.Prefix),
			zap.String("url", parsed.URL))
		return nil, fmt.Errorf("failed to write subscription from quorum: %s", resp.Error)
	}

	options.Logger.Info("created subscription",
		zap.String("prefix", parsed.Prefix),
		zap.String("url", parsed.URL),
		zap.Bool("batch", parsed.Batch),
		zap.Int32("retry_attempts", parsed.RetryAttempts),
		zap.Duration("retry_after_base", parsed.RetryAfterBase))

	return nil, nil
}
