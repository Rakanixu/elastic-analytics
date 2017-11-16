package csv

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/Rakanixu/csv_analysis/db"
)

func NewParser(pathToCVVs string, goroutines int) *Parser {
	return &Parser{
		pathToCVVs: pathToCVVs,
		gourotines: goroutines,
	}
}

type Parser struct {
	pathToCVVs string
	gourotines int
}

var wg sync.WaitGroup

func (n *Parser) ToJSON() {
	n.analyzeCSVs(n.getCSVFiles(n.pathToCVVs))
}

func (n *Parser) getCSVFiles(path string) []string {
	if path != "" {
		path = fmt.Sprintf("%s/*.csv", path)
	} else {
		path = "*.csv"
	}

	files, err := filepath.Glob(path)
	if err != nil {
		log.Fatal(err)
	}

	return files
}

func (n *Parser) analyzeCSVs(paths []string) {
	// Channel buffer equal to number of gourotines
	blocker := make(chan struct{}, n.gourotines)

	for _, v := range paths {
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

			r := csv.NewReader(strings.NewReader(string(b[:count])))

			counter := 0
			var columns []string

			for {
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}

				if counter == 0 {
					columns = record
				} else {
					recMap := make(map[string]interface{})

					for i, k := range columns {
						if k == "Metadata" {
							planifyData(recMap, "Metadata", record[i], 1, counter)
						} else {
							recMap[k] = record[i]
						}
					}

					json, err := json.MarshalIndent(recMap, "", "\t")
					if err != nil {
						log.Fatal(err)
					}
					db.Index(strconv.Itoa(counter), string(json))
				}
				counter++
			}

			// Read from blocker channel to allow next iteration
			<-blocker
			wg.Done()
		}(v)
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
func planifyData(root map[string]interface{}, key string, data interface{}, deep, rowNumber int) {
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
			root[nextKey] = v
		} else {
			if v != nil {
				deep--
				if deep > 0 {
					planifyData(root, nextKey, v, deep, rowNumber)
				}
			}
		}
	}
}
