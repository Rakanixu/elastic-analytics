package db

// Db ...
type Db interface {
	Init(url string) error
	Index(id string, data string) error
	BulkIndex(id string, data interface{})
}

var s Db

// Register ..
func Register(storage Db) {
	s = storage
}

// Init ..
func Init(url string) error {
	return s.Init(url)
}

// Index ...
func Index(id string, data string) error {
	return s.Index(id, data)
}

// BulkIndex ...
func BulkIndex(id string, data interface{}) {
	s.BulkIndex(id, data)
}
