package mergecomplex

// Logger is a fixture type for acceptance testing.
type Logger interface {
	Info(msg string)
}

// Database is a fixture type for acceptance testing.
type Database interface {
	Ping() error
}

// Cache is a fixture type for acceptance testing.
type Cache interface {
	Get(key string) string
}
