package csv

func NewChecksum(checksum string) Checksum {
	return Checksum{
		Checksum: checksum,
	}
}

type Checksum struct {
	Checksum string `json:"checksum"`
}
