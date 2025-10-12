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
	"strconv"
	"time"

	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
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
		Batch:          true, // Default to non-batched
		RetryAttempts:  3,    // Default 3 retries
		RetryAfterBase: 100 * time.Millisecond,
	}

	// Parse optional flags
	for i := 3; i < c.NormalizedLen(); i++ {
		flag, _ := c.SelectNormalizedCommand(i)
		switch flag {
		case "NOBATCH":
			parsed.Batch = false
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

	err = atlas.Subscribe(ctx, consensus.KeyName(parsed.Prefix), parsed.URL, atlas.SubscribeOptions{
		RetryAttempts:  int(parsed.RetryAttempts),
		RetryAfterBase: parsed.RetryAfterBase,
		Auth:           parsed.Auth,
	})
	if err != nil {
		return nil, err
	}

	options.Logger.Info("created subscription",
		zap.String("prefix", parsed.Prefix),
		zap.String("url", parsed.URL),
		zap.Bool("batch", parsed.Batch),
		zap.Int32("retry_attempts", parsed.RetryAttempts),
		zap.Duration("retry_after_base", parsed.RetryAfterBase))

	return nil, nil
}
