package boltkv

type Config struct {
	// File is boltDB file. No need to create it by your own,
	// boltdb driver is able to create the file, or read existing
	File string `mapstructure:"file"`
	// Bucket to store data in boltDB
	bucket string
	// db file permissions
	Permissions int `mapstructure:"permissions"`
	// timeout
	Interval int `mapstructure:"interval"`
}

// InitDefaults initializes default values for the boltdb
func (s *Config) InitDefaults() {
	s.bucket = "default"

	if s.File == "" {
		s.File = "rr.db" // default file name
	}

	if s.Permissions == 0 {
		s.Permissions = 0755
	}

	if s.Interval == 0 {
		s.Interval = 60 // default is 60 seconds timeout
	}
}
