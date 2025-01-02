package atlas

type Options struct {
	DbFilename       string
	MetaFilename     string
	DoReset          bool
	BootstrapConnect string
	ServerId         int64
	Region           string
	AdvertiseAddress string
	AdvertisePort    uint
	ApiKey           string
	SocketPath       string
}

var CurrentOptions *Options

func init() {
	CurrentOptions = &Options{
		DbFilename:       "atlas.db",
		MetaFilename:     "atlas.meta",
		DoReset:          false,
		BootstrapConnect: "",
		ServerId:         0,
		Region:           "local",
		AdvertiseAddress: "localhost",
		AdvertisePort:    8080,
		SocketPath:       "atlas.sock",
	}
}
