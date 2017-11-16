package data

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Rakanixu/elastic-analytics/db"
	"github.com/nu7hatch/gouuid"
)

const (
	PADDING      = 45
	NUM_PADDING  = 9
	CONN_REFUSED = "connection refused"
)

// Data ...
type Data struct {
	date            time.Time
	name            string
	DataRows        []string
	Records         map[string]*Record
	AggHashRecords  map[string][]*Record
	NumTotalColumns int64
	NumTotalRetries int64
}

// Record ...
type Record struct {
	hash                     string
	description              string
	count                    int64
	percentage               float64
	countRetries             int64
	percentageWithoutRetries float64
}

// NewData ...
func NewData(name string) *Data {
	return &Data{
		name:           name,
		Records:        make(map[string]*Record),
		AggHashRecords: make(map[string][]*Record),
	}
}

// NewRecord ...
func NewRecord(description string, hash string) *Record {
	return &Record{
		hash:        hash,
		description: description,
	}
}

// AddRecord ...
func (d *Data) AddRecord(record *Record) {
	if record.hash != "" {
		d.AggHashRecords[record.hash] = append(d.AggHashRecords[record.hash], record)
	}

	if d.Records[record.description] == nil {
		d.Records[record.description] = record
	}
	d.Records[record.description].count++
}

// AddRecord ...
func (d *Data) AddDataRow(dataRow string) {
	d.DataRows = append(d.DataRows, dataRow)
}

// SetTotal ...
func (d *Data) SetTotal(total int64) {
	d.NumTotalColumns = total
}

// Date ...
func (d *Data) Date() {
	var err error

	// Genetare Time from csv file name pattern
	// xxxxxxx-1c2b447a65025fc41ee91b794aa2305a-11042016.csv
	n := strings.Split(d.name, "-")
	s := strings.Replace(n[len(n)-1], ".csv", "", -1)

	s = fmt.Sprintf("%s%s/%s%s/%s%s%s%s",
		string(s[2]), string(s[3]), // MM
		string(s[0]), string(s[1]), // DD
		string(s[4]), string(s[5]), string(s[6]), string(s[7]), // YYYYY
	)

	d.date, err = time.Parse("01/02/2006", s)
	if err != nil {
		log.Println("PARSE CSV TIME ERR", err)
	}
}

// Info ...
func (d *Data) Info() {
	m := make(map[string]int64)

	for _, h := range d.AggHashRecords {
		// Repeated hash, retried
		if len(h) > 1 {
			d.NumTotalRetries += int64(len(h))

			for _, v := range h {
				m[v.description]++
			}
		}
	}

	// Assign to aggregate fields the number of retries
	for k, v := range m {
		d.Records[k].countRetries = v
	}

	for _, v := range d.Records {
		v.percentage = float64(v.count) / float64(d.NumTotalColumns) * 100
		v.percentageWithoutRetries = float64(v.count-v.countRetries) / float64(d.NumTotalColumns-d.NumTotalRetries) * 100
	}
}

// Print ...
func (d *Data) Print() {
	d.Info()

	fmt.Println("\n-------------------------------------------------------------------")
	fmt.Println("DATE:                                       ", d.date)
	fmt.Println("CSV:                                        ", d.name)
	for _, v := range d.Records {
		fmt.Print(v.description)
		if len(v.description)-PADDING < 0 {
			fmt.Printf("\033[%dC", len(v.description)-PADDING)
		} else {
			fmt.Println("")
			fmt.Printf("\033[%dC", PADDING)
		}
		fmt.Printf("%2.3f", v.percentage)
		fmt.Print("%")
		fmt.Printf("\033[%dC", NUM_PADDING-(len(strconv.FormatFloat(v.percentage, 'f', 3, 64))+1))
		fmt.Print(v.count)
		fmt.Printf("\033[%dC", NUM_PADDING-(len(strconv.Itoa(int(v.count)))))
		fmt.Printf("%2.3f", v.percentageWithoutRetries)
		fmt.Print("%")
		fmt.Printf("\033[%dC", NUM_PADDING-(len(strconv.FormatFloat(v.percentageWithoutRetries, 'f', 3, 64))+1))
		fmt.Print(v.count - v.countRetries)
		fmt.Println("")
	}
}

// Export ...
func (d *Data) Export() {
	path := "csv/overview"
	err := os.MkdirAll(path, 0711)
	if err != nil {
		log.Fatal(err)
	}

	n := fmt.Sprintf("%s/%s.csv", path, d.date.String())
	f, err := os.Open(n)
	if err != nil {
		// Create file
		f, err = os.Create(n)
		if err != nil {
			log.Fatal(err)
		}
	}

	defer f.Close()

	// New CSV Writter
	w := csv.NewWriter(f)
	defer w.Flush()

	// Write into the file
	err = w.Write([]string{
		"Description",
		"Percentage with retries",
		"Total count",
		"Percentage without retries",
		"Total count without retries",
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, v := range d.Records {
		err := w.Write([]string{
			v.description,
			strconv.FormatFloat(v.percentage, 'f', 6, 64),
			strconv.Itoa(int(v.count)),
			strconv.FormatFloat(v.percentageWithoutRetries, 'f', 6, 64),
			strconv.Itoa(int(v.count - v.countRetries)),
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (d *Data) ExportDataRows() {
	path := "csv/data"
	err := os.MkdirAll(path, 0711)
	if err != nil {
		log.Fatal(err)
	}

	n := fmt.Sprintf("%s/%s.output.csv", path, d.date.String())
	f, err := os.Open(n)
	if err != nil {
		f, err = os.Create(n)
		if err != nil {
			log.Fatal(err)
		}
	}

	defer f.Close()

	// New CSV Writter
	w := csv.NewWriter(f)
	defer w.Flush()

	// Write into the file
	for _, v := range d.DataRows {
		err = w.Write(strings.Split(v, ","))
		if err != nil {
			log.Fatal(err)
		}
	}
}

// Dump ...
func (d *Data) Dump() error {
	type info struct {
		ID          string  `json:"id"`
		Count       int64   `json:"count"`
		Total       int64   `json:"total"`
		Description string  `json:"description"`
		Percentage  float64 `json:"percentage"`
		Date        string  `json:"date"`
		CsvFile     string  `json:"csv_file"`
	}

	for _, v := range d.Records {
		u, err := uuid.NewV4()
		if err != nil {
			log.Println("ERROR GENRATING UUID", err)
		}

		i := info{
			ID:          u.String(),
			Count:       v.count,
			Total:       d.NumTotalColumns,
			Description: v.description,
			Percentage:  v.percentage,
			Date:        d.date.String(),
			CsvFile:     d.name,
		}

		/*		db.BulkIndex(d.date.String(), )*/

		b, err := json.Marshal(i)
		if err != nil {
			log.Println("ERROR MARSHALLING", err)
		}

		if err := db.Index(u.String(), string(b)); err != nil && !strings.Contains(err.Error(), CONN_REFUSED) {
			log.Println("ERROR INDEXING", err)
		}
	}

	return nil
}

// DataSlice ...
type DataSlice []*Data

// Len ...
func (d DataSlice) Len() int {
	return len(d)
}

// Less ...
func (d DataSlice) Less(i, j int) bool {
	return d[i].date.After(d[j].date)
}

// Swap ...
func (d DataSlice) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
