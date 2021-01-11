package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
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
		log.Println(err)
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
		jaegerReq, err := http.NewRequest("GET", fmt.Sprintf("%s/api/traces/%s?prettyPrint=true", config.JaegerAPI, source["traceID"]), nil)
		log.Println(jaegerReq)
		if err != nil {
			log.Println(err)
		}
		jaegerReq.Header.Set("Content-Type", "application/json")

		jaegerResp, err := http.DefaultClient.Do(jaegerReq)
		if err != nil {
			log.Println(err)
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
		log.Println(traceDat)
		traceArr := traceDat["data"].([]interface{})
		trace := traceArr[0].(map[string]interface{})
		var columnNumber = 1
		var columnDecryptNumber = 1
		var columnChunkNumber = 1
		var columnOrderedChunkNumber = 1
		var ttfbCalculated = false
		var ttfbStart float64
		var ttfb int64
		var decyrptCounter int
		var chunkCounter int
		chunkList := []int64{}
		for j, traceSpanRow := range trace["spans"].([]interface{}) {
			spanRow := traceSpanRow.(map[string]interface{})
			operationName := spanRow["operationName"].(string)

			startDate := time.Unix(int64(spanRow["startTime"].(float64)/1000000), 0)
			duration := time.Duration(int64(spanRow["duration"].(float64))) * time.Microsecond

			column, _ := excelize.ColumnNumberToName(columnNumber)
			columnDecrypt, _ := excelize.ColumnNumberToName(columnDecryptNumber)
			columnChunk, _ := excelize.ColumnNumberToName(columnChunkNumber)
			columnOrderedChunk, _ := excelize.ColumnNumberToName(columnOrderedChunkNumber)

			if operationName == "cachecash.com/Client/GetObject" {
				if i == 0 {
					title := "Timestamp (CET)"
					setHeader(f, "timestamp", title, column, i+1, 0)
					setHeader(f, "timestamp", title, columnDecrypt, len(hitsChild)+4+i+1, 0)
					setHeader(f, "timestamp", title, columnChunk, (len(hitsChild)+4)*2+i+1, 0)
					setHeader(f, "timestamp", title, columnOrderedChunk, (len(hitsChild)+4)*3+i+1, 0)
				}

				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+3), startDate.String())
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnDecrypt, len(hitsChild)+4+i+3), startDate.String())
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnChunk, (len(hitsChild)+4)*2+i+3), startDate.String())
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnOrderedChunk, (len(hitsChild)+4)*3+i+3), startDate.String())
				columnNumber++
				columnDecryptNumber++
				columnChunkNumber++
				columnOrderedChunkNumber++
				column, _ = excelize.ColumnNumberToName(columnNumber)
				columnDecrypt, _ := excelize.ColumnNumberToName(columnDecryptNumber)
				columnChunk, _ := excelize.ColumnNumberToName(columnChunkNumber)
				columnOrderedChunk, _ = excelize.ColumnNumberToName(columnOrderedChunkNumber)

				if i == 0 {
					title := "Duration (s)"
					setHeader(f, "", title, column, i+1, 0)
					setHeader(f, "", title, columnDecrypt, len(hitsChild)+4+i+1, 0)
					setHeader(f, "", title, columnChunk, (len(hitsChild)+4)*2+i+1, 0)
					setHeader(f, "", title, columnOrderedChunk, (len(hitsChild)+4)*3+i+1, 0)
				}
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+3), duration.String())
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnDecrypt, len(hitsChild)+4+i+3), duration.String())
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnChunk, (len(hitsChild)+4)*2+i+3), duration.String())
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnOrderedChunk, (len(hitsChild)+4)*3+i+3), duration.String())
				columnNumber++
				columnDecryptNumber++
				columnChunkNumber++
				columnOrderedChunkNumber++
			} else if operationName == "cachecash.com/Client/requestBundle" {
				if !ttfbCalculated {
					if i == 0 {
						title := "~Publisher TTFB (ms)"
						setHeader(f, "", title, column, i+1, 0)
					}

					f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+3), duration.Milliseconds())
					columnNumber++
				}
				chunkCounter = 0
			} else if operationName == "cachecash.com/Client/HandleBundle" {
				if !ttfbCalculated {
					ttfbStart = spanRow["startTime"].(float64)
				}
			} else if operationName == "cachecash.com/Client/decryptPuzzle" {
				if !ttfbCalculated {
					if i == 0 {
						title := "Caches TTFB (ms)"
						setHeader(f, "", title, column, i+1, 0)
					}

					ttfb = int64((spanRow["startTime"].(float64)-ttfbStart)/1000) + duration.Milliseconds()
					f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+3), ttfb)
					ttfbCalculated = true
					columnNumber++
				}

				column, _ = excelize.ColumnNumberToName(columnNumber)
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+3), duration.Milliseconds())
				if decyrptCounter < 3 {
					if i == 0 {
						title := fmt.Sprintf("%d.", decyrptCounter+1)
						setHeader(f, "", title, column, i+1, 0)
					}
					columnNumber++
				} else if i == 0 {
					title := "Last"
					setHeader(f, "puzzle", title, column, i+1, 3)
				}

				columnDecrypt, _ = excelize.ColumnNumberToName(columnDecryptNumber)
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnDecrypt, len(hitsChild)+4+i+3), duration.Milliseconds())
				if i == 0 {
					title := fmt.Sprintf("%d. Decrypt", decyrptCounter+1)
					setHeader(f, "", title, columnDecrypt, len(hitsChild)+4+i+1, 0)
				}
				decyrptCounter++
				columnDecryptNumber++
			} else if operationName == "cachecash.com/Client/requestChunk" {
				chunkCounter++
				columnChunk, _ = excelize.ColumnNumberToName(columnChunkNumber)
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnChunk, (len(hitsChild)+4)*2+i+3), duration.Milliseconds())
				columnChunkNumber++

				if len(chunkList) < 4 {
					chunkList = append(chunkList, duration.Milliseconds())
				}
				if len(chunkList) == 4 {
					sort.Slice(chunkList, func(k, l int) bool { return chunkList[k] < chunkList[l] })

					columnOrderedChunkNumber = columnOrderedChunkNumber - 3
					for _, chunkRow := range chunkList {
						columnOrderedChunk, _ = excelize.ColumnNumberToName(columnOrderedChunkNumber)
						f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnOrderedChunk, (len(hitsChild)+4)*3+i+3), chunkRow)
						columnOrderedChunkNumber++
					}
					chunkList = []int64{}
				} else {
					columnOrderedChunkNumber++
				}

				if i == 0 {
					if chunkCounter == 12 {
						title := "Last"
						setHeader(f, "chunk", title, columnChunk, (len(hitsChild)+4)*2+i+1, chunkCounter-1)
					} else {
						title := fmt.Sprintf("%d.", chunkCounter)
						setHeader(f, "", title, columnChunk, (len(hitsChild)+4)*2+i+1, 0)
					}
				}

			}

			if j == len(trace["spans"].([]interface{}))-1 && len(chunkList) != 0 {
				sort.Slice(chunkList, func(k, l int) bool { return chunkList[k] < chunkList[l] })
				columnOrderedChunkNumber = columnOrderedChunkNumber - len(chunkList)
				for _, chunkRow := range chunkList {
					columnOrderedChunk, _ = excelize.ColumnNumberToName(columnOrderedChunkNumber)
					f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnOrderedChunk, (len(hitsChild)+4)*3+i+3), chunkRow)
					columnOrderedChunkNumber++
				}
				chunkList = []int64{}
				/*if chunkCounter != 12 {
					title := "Last"
					setHeader(f, "chunk", title, columnChunk, (len(hitsChild)+4)*3+i+1, chunkCounter -1)
				}
				chunkCounter = 0*/
			}
		}
	}
	f.SaveAs(fmt.Sprintf("./%v.xlsx", string(*jaegerIndex)))
}

func setHeader(f *excelize.File, kind, title, col string, row, merge int) {
	headerStyle, _ := f.NewStyle(`{"fill":{"type":"pattern","color":["#b1cefc"],"pattern":1},"font":{"bold":true},"alignment":{"wrap_text":true}}`)
	f.NewStyle(headerStyle)
	style, _ := f.NewStyle(`{"fill":{"type":"pattern","color":["#E6E6FA"],"pattern":1},"font":{"bold":true},"alignment":{"wrap_text":true}}`)
	f.NewStyle(style)

	f.SetCellStyle("Sheet1", fmt.Sprintf("%s%d", col, row+1), fmt.Sprintf("%s%d", "B", row+1), headerStyle)
	f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", col, row+1), title)
	f.SetColWidth("Sheet1", col, col, 14)

	if kind == "timestamp" {
		f.SetCellStyle("Sheet1", fmt.Sprintf("%s%d", col, row), fmt.Sprintf("%s%d", col, row), style)
		f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", col, row), "Transactions")

		f.SetColWidth("Sheet1", string(col), string(col), 28)
	} else if kind == "puzzle" || kind == "chunk" {
		colNumber, _ := excelize.ColumnNameToNumber(col)
		colStart, _ := excelize.ColumnNumberToName(colNumber - merge)
		f.MergeCell("Sheet1", fmt.Sprintf("%s%d", colStart, row), fmt.Sprintf("%s%d", col, row))
		f.SetCellStyle("Sheet1", fmt.Sprintf("%s%d", colStart, row), fmt.Sprintf("%s%d", colStart, row), style)
		if kind == "puzzle" {
			f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", colStart, row), "Decrypt Puzzle Duration (ms)")
		} else if kind == "chunk" {
			f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", colStart, row), "Chunk Group in Bundle")
		}
	}
}
