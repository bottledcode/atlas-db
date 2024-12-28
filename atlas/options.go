package atlas

type Options struct {
	DbFilename       string
	MetaFilename     string
	DoReset          bool
	BootstrapConnect string
	ServerId         int
	ApiKey           string
}

var CurrentOptions *Options

func init() {
	CurrentOptions = &Options{
		DbFilename:       "atlas.db",
		MetaFilename:     "atlas.meta",
		DoReset:          false,
		BootstrapConnect: "",
		ServerId:         0,
	}
}
