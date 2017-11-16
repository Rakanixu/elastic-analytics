package csv

import (
	"crypto/md5"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Rakanixu/elastic-analytics/db"
)

func NewParser(pathToCVVs string, goroutines int) *Parser {
	return &Parser{
		pathToCVVs: pathToCVVs,
		gourotines: goroutines,
		md5:        md5.New(),
	}
}

type Parser struct {
	pathToCVVs string
	gourotines int
	files      []string
	md5        hash.Hash
}

func (n *Parser) ToJSON() {
	n.getCSVFiles(n.pathToCVVs)
	n.checkFiles()
	n.processCSVs()
}

func (n *Parser) getCSVFiles(path string) {
	if path != "" {
		path = fmt.Sprintf("%s/*.csv", path)
	} else {
		path = "*.csv"
	}

	files, err := filepath.Glob(path)
	if err != nil {
		log.Fatal(err)
	}

	n.files = files
}

func (n *Parser) checkFiles() {
	for _, v := range n.files {
		f, err := os.Open(v)
		if err != nil {
			log.Fatal("Error opening CSV files", err)
		}
		defer f.Close()

		var beginning [1000]byte
		_, err = io.ReadFull(f, beginning[:])
		if err != nil {
			log.Fatal("Error checking CSV file", err)
		}

		_, err = n.md5.Write(beginning[:])
		if err != nil {
			log.Fatal("Error checking CSV checksum", err)
		}

		checksum := fmt.Sprintf("%x", n.md5.Sum(nil))

		db.Index(checksum, db.FILE_INDEX, fmt.Sprintf("{\"checksum\": \"%s\"}", checksum))
		log.Println(checksum)
		n.md5.Reset()
	}
}

func (n *Parser) processCSVs() {
	var wg sync.WaitGroup
	// Channel buffer equal to number of gourotines
	blocker := make(chan struct{}, n.gourotines)

	for _, v := range n.files {
		// Fill blocker channel
		blocker <- struct{}{}
		wg.Add(1)

		go func(path string) {
			s, err := os.Open(path)
			if err != nil {
				log.Fatal(err)
			}
			defer s.Close()

			fi, err := s.Stat()
			log.Println("Process:", fi.Name(), "Bytes:", fi.Size())

			b := make([]byte, fi.Size())
			count, err := s.Read(b)
			if err != nil {
				log.Fatal(err)
			}

			n.convertToJSON(string(b[:count]))

			// Read from blocker channel to allow next iteration
			<-blocker
			wg.Done()
		}(v)
	}

	wg.Wait()
}

func (n *Parser) convertToJSON(file string) {
	var wg sync.WaitGroup
	var mutex = &sync.Mutex{}
	blocker := make(chan struct{}, 16)
	r := csv.NewReader(strings.NewReader(file))
	counter := 0
	var columns []string

	for {
		blocker <- struct{}{}
		wg.Add(1)
		record, err := r.Read()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatal(err)
		}

		go func(counter int, record []string) {
			if counter == 0 {
				columns = record
			} else {
				recMap := make(map[string]interface{})

				for i, k := range columns {
					if k == "Metadata" {
						planifyData(mutex, recMap, "Metadata", record[i], 1, counter)
					} else {
						mutex.Lock()
						recMap[k] = record[i]
						mutex.Unlock()
					}
				}

				json, err := json.MarshalIndent(recMap, "", "\t")
				if err != nil {
					log.Fatal(err)
				}

				db.Index( /* string(n.md5.Sum(json[0:100])) */ "", db.DEFAULT_INDEX, string(json))
				//n.md5.Reset()
			}

			<-blocker
			wg.Done()
		}(counter, record)

		counter++
	}

	wg.Wait()
}

/**
* Panifies a CSV row to a flat JSON.
*	root holds all row attributes and subattributes
*	key is the JSON attribute name
* data is a fragment of the row.
* deep sets how many recursive iterations will be done over a data fragment
* rowNumber is the row number in the CSV file
 */
func planifyData(mutex *sync.Mutex, root map[string]interface{}, key string, data interface{}, deep, rowNumber int) {
	m := make(map[string]interface{})

	str, ok := data.(string)
	if ok {
		err := json.Unmarshal([]byte(str), &m)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		m, ok = data.(map[string]interface{})
		if !ok || data == nil {
			log.Println("Cannot parse JSON from row ", rowNumber, key, data)
			return
		}
	}

	for k, v := range m {
		nextKey := fmt.Sprintf("%s.%s", key, k)
		_, okStr := v.(string)
		_, okNum := v.(float64)
		_, okBool := v.(bool)
		if okStr || okNum || okBool {
			mutex.Lock()
			root[nextKey] = v
			mutex.Unlock()
		} else {
			if v != nil {
				deep--
				if deep > 0 {
					planifyData(mutex, root, nextKey, v, deep, rowNumber)
				}
			}
		}
	}
}
