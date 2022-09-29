package main

import (
	"encoding/json"
	es "github.com/elastic/go-elasticsearch/v7"
	"go_project/conf"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)


func main() {
	log.Printf("application runs\n")

	yamlFile, err := os.ReadFile("./config.yml")
	if err != nil {
		log.Fatal(err)
	}
	task := conf.Task{}
	err = yaml.Unmarshal(yamlFile, &task)
	log.Println(string(yamlFile))

	cleanExpiredIndex(task)
	// 启动定时任务
	ticker := time.NewTicker(time.Duration(task.TaskPeriod) * time.Hour)
	go func() {
		for {
			select {
			case <- ticker.C:
				cleanExpiredIndex(task)
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}

// 删除过期索引
func cleanExpiredIndex(task conf.Task) {
	log.Println("task runs")

	var err error
	client, err := es.NewClient(es.Config{
		Addresses: []string{task.EsUrl},
		Username:  task.Username,
		Password:  task.Password,
	})
	if err != nil {
		log.Fatal(err)
		return
	}

	var d map[string]interface{}
	resp, err := client.Indices.Get([]string{task.IndexPattern})
	if err != nil {
		log.Fatal(err)
		return
	}
	json.NewDecoder(resp.Body).Decode(&d)

	loc, _ := time.LoadLocation("Asia/Shanghai")
 	now := time.Now()
	var delIndices = make([]string, 0, 32)
	for indexName, _ := range d {
		arr := strings.Split(indexName, "-")
		if len(arr) < 3 {
			continue
		}
		indexTimeStr := arr[2]

		indexTime, err := time.ParseInLocation("2006.01.02", indexTimeStr, loc)
		if err != nil {
			log.Fatal(err)
		}
 		if now.Sub(indexTime) > time.Duration(task.RetainDays) * 24 * time.Hour {
			delIndices = append(delIndices, indexName)
		}
	}

	// 删除索引
	if len(delIndices) > 0 {
		log.Printf("delete %s\n", delIndices)
		resp, _ = client.Indices.Delete(delIndices)
		log.Println(resp)
	}

}