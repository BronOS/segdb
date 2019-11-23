package apiserver

// Config ...
type Config struct {
	LogLevel    string `toml:"log_level"`
	StoragePath string `toml:"storage_path"`
	BindAddr    string
}

// NewConfig ...
func NewConfig(bindAddr string) *Config {
	return &Config{
		LogLevel:    "debug",
		StoragePath: "var/lib/segdb",
		BindAddr:    bindAddr,
	}
}
