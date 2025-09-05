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

package options

import (
	"os"
	"sync"

	"go.uber.org/zap"
)

var Logger *zap.Logger

type Options struct {
	DbFilename                   string
	MetaFilename                 string
	DoReset                      bool
	BootstrapConnect             string
	ServerId                     int64
	Region                       string
	AdvertiseAddress             string
	AdvertisePort                uint
	ApiKey                       string
	SocketPath                   string
	DevelopmentMode              bool
	toleratedZoneFailures        int64
	toleratedNodePerZoneFailures int64
	mu                           sync.RWMutex
}

func (o *Options) GetFn() int64 {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.toleratedNodePerZoneFailures
}

func (o *Options) GetFz() int64 {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.toleratedZoneFailures
}

func (o *Options) SetFn(fn int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.toleratedNodePerZoneFailures = fn
}

func (o *Options) SetFz(fz int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.toleratedZoneFailures = fz
}

var CurrentOptions *Options

func init() {
	developmentMode := os.Getenv("ATLAS_DEVELOPMENT_MODE") == "true"

	CurrentOptions = &Options{
		DbFilename:                   "atlas.db",
		MetaFilename:                 "atlas.meta",
		DoReset:                      false,
		BootstrapConnect:             "",
		ServerId:                     0,
		Region:                       "local",
		AdvertiseAddress:             "localhost",
		AdvertisePort:                8080,
		SocketPath:                   "atlas.sock",
		DevelopmentMode:              developmentMode,
		toleratedZoneFailures:        1,
		toleratedNodePerZoneFailures: 1,
	}
}
