package atlas

import "sync"

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
		toleratedZoneFailures:        1,
		toleratedNodePerZoneFailures: 1,
	}
}
