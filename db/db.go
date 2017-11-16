package db

const (
	DEFAULT_INDEX = "index"
	FILE_INDEX    = "files_checksum"
	DEFAULT_TYPE  = "record"
)

// Db ...
type Db interface {
	Init(url string) error
	Index(id string, index string, data string) error
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
func Index(id string, index string, data string) error {
	return s.Index(id, index, data)
}

// BulkIndex ...
func BulkIndex(id string, data interface{}) {
	s.BulkIndex(id, data)
}
