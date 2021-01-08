package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	excelize "github.com/360EntSecGroup-Skylar/excelize/v2"
	yaml "gopkg.in/yaml.v2"
)

type config struct {
	ESUsername string `yaml:"es_username"`
	ESPassword string `yaml:"es_password"`
	ESAPI      string `yaml:"es_api"`
	JaegerAPI  string `yaml:"jaeger_api"`
}

func main() {
	file, err := os.Open("config.yaml")
	if err != nil {
		log.Printf("Config: unexpected error executing command: %v", err)
		return
	}
	decoder := yaml.NewDecoder(file)
	var config config
	err = decoder.Decode(&config)
	if err != nil {
		log.Printf("Mailer: unexpected error executing command: %v", err)
		return
	}

	jaegerIndex := flag.String("index", "jaeger-span-2021-01-06", "index name")
	flag.Parse()
	log.Println(string(*jaegerIndex))
	body := strings.NewReader(`{ "from": 0, "size": 500, "sort": [{"startTime": {"order": "asc"}}], "query": { "bool": { "must": [{"match": { "operationName": "cachecash.com/Client/GetObject" }}], "filter": {"range": { "duration": { "gte": 0, "lte": 50000000 }}}}}}`)
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%v/_search?pretty", config.ESAPI, string(*jaegerIndex)), body)
	log.Println(req)
	if err != nil {
		// handle err
	}
	req.SetBasicAuth(config.ESUsername, config.ESPassword)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// handle err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()

	f := excelize.NewFile()
	var dat map[string]interface{}
	//var hits map[string]interface{}
	if err := json.Unmarshal(respBody, &dat); err != nil {
		panic(err)
	}
	hits := dat["hits"].(map[string]interface{})
	hitsChild := hits["hits"].([]interface{})
	for i, row := range hitsChild {
		hitRow := row.(map[string]interface{})
		source := hitRow["_source"].(map[string]interface{})

		if i == 0 {
			jaegerReq, err := http.NewRequest("GET", fmt.Sprintf("%s/api/traces/%s?prettyPrint=true", config.JaegerAPI, source["traceID"]), nil)
			if err != nil {
				// handle err
			}
			jaegerReq.Header.Set("Content-Type", "application/json")

			jaegerResp, err := http.DefaultClient.Do(jaegerReq)
			if err != nil {
				// handle err
			}
			jaegerRespBody, err := ioutil.ReadAll(jaegerResp.Body)
			if err != nil {
				log.Fatalln(err)
			}
			defer jaegerResp.Body.Close()

			var traceDat map[string]interface{}
			//var hits map[string]interface{}
			if err := json.Unmarshal(jaegerRespBody, &traceDat); err != nil {
				panic(err)
			}
			traceArr := traceDat["data"].([]interface{})
			trace := traceArr[0].(map[string]interface{})
			for _, traceSpanRow := range trace["spans"].([]interface{}) {
				var j = 1
				var ttfbDone = false
				var ttfbStart int64
				//var ttfb int
				spanRow := traceSpanRow.(map[string]interface{})
				operationName := spanRow["operationName"].(string)

				startDate := time.Unix(int64(spanRow["startTime"].(float64)/1000000), 0)
				duration := time.Duration(int64(spanRow["duration"].(float64))) * time.Microsecond

				column, _ := excelize.ColumnNumberToName(j + 2)
				//var duration time.Duration = time.Duration
				if operationName == "cachecash.com/Client/GetObject" {
					f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+1), duration.String())
					column, _ = excelize.ColumnNumberToName(j + 1)
					f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+1), startDate.String())
					j++
				} else if operationName == "cachecash.com/Client/requestBundle" {
					f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+1), duration.Milliseconds())
					j++
				} else if operationName == "cachecash.com/Client/HandleBundle" {
					if !ttfbDone {
						ttfbStart = int64(spanRow["startTime"].(float64) / 1000000)
						log.Println(ttfbStart)
					}
				} else if operationName == "cachecash.com/Client/decryptPuzzle" {
					if !ttfbDone {

					}
					f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+1), duration.Milliseconds())
					j += 2
				}

			}
		}
	}
	f.SaveAs(fmt.Sprintf("./%v.xlsx", string(*jaegerIndex)))
}
