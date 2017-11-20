package db

const (
	DEFAULT_INDEX       = "index"
	FILE_CHECKSUM_INDEX = "files_checksum"
	DEFAULT_TYPE        = "record"
	FILE_CHECKSUM_TYPE  = "files_checksum_type"
)

// Db ...
type Db interface {
	Init(url string) error
	Index(id string, index string, docType string, data string) error
	BulkIndex(id string, data interface{})
	Search(index string, docType string, query interface{}) ([]interface{}, error)
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
func Index(id string, index string, docType string, data string) error {
	return s.Index(id, index, docType, data)
}

// BulkIndex ...
func BulkIndex(id string, data interface{}) {
	s.BulkIndex(id, data)
}

// Search ...
func Search(index string, docType string, query interface{}) ([]interface{}, error) {
	return s.Search(index, docType, query)
}
