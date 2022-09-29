package es

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"strings"
	"testing"
	"time"

	es "github.com/elastic/go-elasticsearch/v7"
)

var (
	client *es.Client
)

func init() {
	var err error
	client, err = es.NewClient(es.Config{
		Addresses: []string{"http://172.16.101.71:9200"},
		Username:  "elastic",
		Password:  "sWk~Yt=#P9IL",
	})
	if err != nil {
		log.Fatal(err)
	}
}

func TestNewESClient(t *testing.T) {
	t.Log(client.Info())
}

func TestCreateIndex(t *testing.T) {
	a := assert.New(t)
	response, err := client.Indices.Create("book-0.1.0", client.Indices.Create.WithBody(strings.NewReader(`
	{
		"aliases": {
			"book":{}
		},
		"settings": {
			"analysis": {
				"normalizer": {
					"lowercase": {
						"type": "custom",
						"char_filter": [],
						"filter": ["lowercase"]
					}
				}
			}
		},
		"mappings": {
			"properties": {
				"name": {
					"type": "keyword",
					"normalizer": "lowercase"
				},
				"price": {
					"type": "double"
				},
				"summary": {
					"type": "text",
					"analyzer": "ik_max_word"
				},
				"author": {
					"type": "keyword"
				},
				"pubDate": {
					"type": "date"
				},
				"pages": {
					"type": "integer"
				}
			}
		}
	}
	`)))
	a.Nil(err)
	t.Log(response)
}

type Book struct {
	ID		int
	Author  string     `json:"author"`
	Name    string     `json:"name"`
	Pages   int        `json:"pages"`
	Price   float64    `json:"price"`
	PubDate *time.Time `json:"pubDate"`
	Summary string     `json:"summary"`
}

func TestCreateDocument(t *testing.T) {
	a := assert.New(t)
	body := &bytes.Buffer{}
	pubDate := time.Now()
	err := json.NewEncoder(body).Encode(&Book{
		Author:  "金庸",
		Price:   96.0,
		Name:    "天龙八部",
		Pages:   1978,
		PubDate: &pubDate,
		Summary: "...",
	})
	a.Nil(err)
	response, err := client.Create("book", "10001", body)
	a.Nil(err)
	t.Log(response)
}

func TestGetDocument(t *testing.T) {
	a := assert.New(t)
	response, err := client.Get("book", "10001")
	a.Nil(err)
	t.Log(response)
}

func TestBulk(t *testing.T) {
	createBooks := []*Book{
		{
			ID:     10002,
			Name:   "神雕侠侣",
			Author: "金庸",
		},
		{
			ID:     10003,
			Name:   "射雕英雄传",
			Author: "金庸",
		},
	}
	deleteBookIds := []int{10001}

	a := assert.New(t)
	body := &bytes.Buffer{}
	for _, book := range createBooks {
		meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, book.ID, "\n"))
		data, err := json.Marshal(book)
		a.Nil(err)
		data = append(data, "\n"...)
		body.Grow(len(meta) + len(data))
		body.Write(meta)
		body.Write(data)
	}
	for _, id := range deleteBookIds {
		meta := []byte(fmt.Sprintf(`{ "delete" : { "_id" : "%d" } }%s`, id, "\n"))
		body.Grow(len(meta))
		body.Write(meta)
	}
	t.Log(body.String())

	response, err := client.Bulk(body, client.Bulk.WithIndex("book"))
	a.Nil(err)
	t.Log(response)
}

func TestSearch(t *testing.T) {
	a := assert.New(t)
	body := &bytes.Buffer{}
	body.WriteString(`
	{
		"_source":{
		  "excludes": ["author"]
		}, 
		"query": {
		  "match_phrase": {
			"author": "金庸"
		  }
		},
		"sort": [
		  {
			"pages": {
			  "order": "desc"
			}
		  }
		], 
		"from": 0,
		"size": 5
	}
	`)
	response, err := client.Search(client.Search.WithIndex("book"), client.Search.WithBody(body))
	a.Nil(err)
	t.Log(response)
}