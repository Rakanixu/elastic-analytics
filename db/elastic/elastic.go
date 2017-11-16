package elastic

import (
	"context"
	"log"
	"time"

	"github.com/Rakanixu/elastic-analytics/db"
	elib "gopkg.in/olivere/elastic.v5"
)

const (
	DEFAULT_INDEX = "index"
	DEFAULT_TYPE  = "record"
)

type elastic struct {
	Client        *elib.Client
	BulkProcessor *elib.BulkProcessor
}

func init() {
	db.Register(new(elastic))
}

func (e *elastic) Init(url string) error {
	var err error
	if url == "" {
		url = "http://localhost:9200"
	}

	// Client
	e.Client, err = elib.NewSimpleClient(
		elib.SetURL(url),
		//elib.SetBasicAuth(username, password),
		elib.SetMaxRetries(3),
	)
	if err != nil {
		return err
	}

	// Bulk Processor, used for users and channels
	e.BulkProcessor, err = e.Client.BulkProcessor().
		After(func(executionId int64, requests []elib.BulkableRequest, response *elib.BulkResponse, err error) {
			log.Println(executionId)
			log.Println(requests)
			log.Println(response)
			log.Println(err)
			log.Println()
		}).
		Workers(3).
		BulkActions(1000).               // commit if # requests >= 1000
		FlushInterval(10 * time.Second). // commit every 10s
		Do(context.Background())
	if err != nil {
		return err
	}

	log.Println("Initialized ElasticSearch on ", url)

	return nil
}

func (e *elastic) Index(id string, data string) error {
	ctx := context.Background()
	exists, err := e.Client.IndexExists(DEFAULT_INDEX).Do(ctx)
	if err != nil {
		return err
	}

	if !exists {
		_, err := e.Client.CreateIndex(DEFAULT_INDEX).Do(ctx)
		if err != nil {
			return err
		}
	}

	_, err = e.Client.Index().Index(DEFAULT_INDEX).Type(DEFAULT_TYPE).Id(id).BodyString(data).Do(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (e *elastic) BulkIndex(id string, data interface{}) {
	log.Println("BULKINDEX", id, data)

	r := elib.NewBulkUpdateRequest().
		Index(DEFAULT_INDEX).
		Type(DEFAULT_TYPE).
		Id(id).
		DocAsUpsert(true).
		Doc(data)

	e.BulkProcessor.Add(r)
}
