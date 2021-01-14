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

type handleOperations struct {
	Chunk  []int64
	Puzzle int64
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
	body := strings.NewReader(`{ "from": 0, "size": 1500, "sort": [{"startTime": {"order": "asc"}}], "query": { "bool": { "must": [{"match": { "operationName": "cachecash.com/Client/GetObject" }}], "filter": {"range": { "duration": { "gte": 0, "lte": 50000000 }}}}}}`)
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%v/_search?pretty", config.ESAPI, string(*jaegerIndex)), body)
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
		traceArr := traceDat["data"].([]interface{})
		trace := traceArr[0].(map[string]interface{})

		// CATEGORIZE
		headers := false
		if i == 0 {
			headers = true
		}
		var columnNumber = 1
		var columnDecryptNumber = 1
		var columnChunkNumber = 1
		var columnOrderedChunkNumber = 1
		var ttfbCalculated = false
		var ttfbStart float64
		var ttfb int64
		var decyrptCounter int
		groupedChunkList := map[string]*handleOperations{}
		handleBundleList := []string{}
		handleBundleGroup := []int{}
		requestBundleCounter := -1
		for _, traceSpanRow := range trace["spans"].([]interface{}) {
			spanRow := traceSpanRow.(map[string]interface{})
			operationName := spanRow["operationName"].(string)

			startDate := time.Unix(int64(spanRow["startTime"].(float64)/1000000), 0)
			duration := time.Duration(int64(spanRow["duration"].(float64))) * time.Microsecond

			column, _ := excelize.ColumnNumberToName(columnNumber)
			columnDecrypt, _ := excelize.ColumnNumberToName(columnDecryptNumber)
			columnChunk, _ := excelize.ColumnNumberToName(columnChunkNumber)
			columnOrderedChunk, _ := excelize.ColumnNumberToName(columnOrderedChunkNumber)

			if operationName == "cachecash.com/Client/GetObject" {
				if headers {
					title := "Transactions"
					setHeader(f, "transaction", title, column, i+1, 0)
					setHeader(f, "transaction", title, columnDecrypt, len(hitsChild)+4+i+1, 0)
					setHeader(f, "transaction", title, columnChunk, (len(hitsChild)+4)*2+i+1, 0)
					setHeader(f, "transaction", title, columnOrderedChunk, (len(hitsChild)+4)*3+i+1, 0)
					title = "Timestamp (CET)"
					setHeader(f, "timestamp", title, column, i+2, 0)
					setHeader(f, "timestamp", title, columnDecrypt, len(hitsChild)+4+i+2, 0)
					setHeader(f, "timestamp", title, columnChunk, (len(hitsChild)+4)*2+i+2, 0)
					setHeader(f, "timestamp", title, columnOrderedChunk, (len(hitsChild)+4)*3+i+2, 0)
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

				if headers {
					title := "Duration (s)"
					setHeader(f, "", title, column, i+2, 0)
					setHeader(f, "", title, columnDecrypt, len(hitsChild)+4+i+2, 0)
					setHeader(f, "", title, columnChunk, (len(hitsChild)+4)*2+i+2, 0)
					setHeader(f, "", title, columnOrderedChunk, (len(hitsChild)+4)*3+i+2, 0)
				}
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+3), duration.Seconds())
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnDecrypt, len(hitsChild)+4+i+3), duration.Seconds())
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnChunk, (len(hitsChild)+4)*2+i+3), duration.Seconds())
				f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnOrderedChunk, (len(hitsChild)+4)*3+i+3), duration.Seconds())
				columnNumber++
				columnDecryptNumber++
				columnChunkNumber++
				columnOrderedChunkNumber++
			} else if operationName == "cachecash.com/Client/requestBundle" {
				if !ttfbCalculated {
					if headers {
						title := "~Publisher TTFB (ms)"
						setHeader(f, "", title, column, i+2, 0)
					}
					f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+3), duration.Milliseconds())
					columnNumber++
				}
				handleBundleGroup = append(handleBundleGroup, 0)
				requestBundleCounter++
			} else if operationName == "cachecash.com/Client/HandleBundle" {
				if !ttfbCalculated {
					ttfbStart = spanRow["startTime"].(float64)
				}
				handleBundleGroup[requestBundleCounter]++
				handleBundleList = append(handleBundleList, spanRow["spanID"].(string))
				groupedChunkList[spanRow["spanID"].(string)] = &handleOperations{}
			} else if operationName == "cachecash.com/Client/decryptPuzzle" {
				references := spanRow["references"].([]interface{})
				for _, refRowInt := range references {
					refRow := refRowInt.(map[string]interface{})
					if refRow["refType"].(string) == "CHILD_OF" {
						groupedChunkList[refRow["spanID"].(string)].Puzzle = duration.Milliseconds()

						if handleBundleList[0] == refRow["spanID"].(string) && !ttfbCalculated {
							if headers {
								title := "Caches TTFB (ms)"
								setHeader(f, "", title, column, i+2, 0)
							}

							ttfb = int64((spanRow["startTime"].(float64)-ttfbStart)/1000) + duration.Milliseconds()
							f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+3), ttfb)
							ttfbCalculated = true
							columnNumber++
						}
					}
				}
			} else if operationName == "cachecash.com/Client/requestChunk" {
				references := spanRow["references"].([]interface{})
				for _, refRowInt := range references {
					refRow := refRowInt.(map[string]interface{})
					if refRow["refType"].(string) == "CHILD_OF" {
						groupedChunkList[refRow["spanID"].(string)].Chunk = append(groupedChunkList[refRow["spanID"].(string)].Chunk, duration.Milliseconds())
					}
				}
			}
		}

		chunkCounter := 1
		for hn, handleBundle := range handleBundleList {
			// PUZZLE
			decryptDuration := groupedChunkList[handleBundle].Puzzle
			// FIRST SECTION
			column, _ := excelize.ColumnNumberToName(columnNumber)
			f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", column, i+3), decryptDuration)
			if decyrptCounter < 3 {
				if headers {
					title := fmt.Sprintf("%d.", decyrptCounter+1)
					setHeader(f, "", title, column, i+2, 0)
				}
				columnNumber++
			} else if headers {
				title := "Last"
				setHeader(f, "", title, column, i+2, 0)
				title = "Decrypt Puzzle Duration (ms)"
				setHeader(f, "puzzle", title, column, i+1, 3)
			}
			// PUZZLE DECRYPTION LIST
			columnDecrypt, _ := excelize.ColumnNumberToName(columnDecryptNumber)
			f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnDecrypt, len(hitsChild)+4+i+3), decryptDuration)
			if headers {
				title := fmt.Sprintf("%d. Decrypt", decyrptCounter+1)
				setHeader(f, "", title, columnDecrypt, len(hitsChild)+4+i+2, 0)
			}
			decyrptCounter++
			columnDecryptNumber++

			// CHUNKS
			groupedChunks := groupedChunkList[handleBundle].Chunk
			handleChunkGroups(f, groupedChunks, handleBundleGroup, (len(hitsChild)+4)*3+i, columnChunkNumber, hn, chunkCounter, headers)
			sortChunkGroup := groupedChunks
			sort.Slice(sortChunkGroup, func(k, l int) bool { return sortChunkGroup[k] < sortChunkGroup[l] })
			columnChunkNumber, chunkCounter = handleChunkGroups(f, sortChunkGroup, handleBundleGroup, (len(hitsChild)+4)*2+i, columnChunkNumber, hn, chunkCounter, headers)
		}
	}

	f.SaveAs(fmt.Sprintf("./%v.xlsx", string(*jaegerIndex)))
}

func handleChunkGroups(f *excelize.File, groupedChunks []int64, handleBundleGroup []int, row, column, handleNumber, chunkCounter int, headers bool) (int, int) {
	for key, chunkDuration := range groupedChunks {
		columnChunk, _ := excelize.ColumnNumberToName(column)
		f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", columnChunk, row+3), chunkDuration)
		if headers {
			title := fmt.Sprintf("%d.", chunkCounter)
			setHeader(f, "", title, columnChunk, row+2, 0)
			if checkBundleChange(handleBundleGroup, handleNumber+1) && key == len(groupedChunks)-1 {
				title = "Chunks in Bundle"
				setHeader(f, "bundle-chunks", title, columnChunk, row, chunkCounter-1)
				title = fmt.Sprintf("%d. Handle Bundle", handleNumber+1)
				setHeader(f, "handle-chunks", title, columnChunk, row+1, len(groupedChunks)-1)
				chunkCounter = 0
			} else if key == len(groupedChunks)-1 {
				title = fmt.Sprintf("%d. Handle Bundle", handleNumber+1)
				setHeader(f, "handle-chunks", title, columnChunk, row+1, len(groupedChunks)-1)
			}
		}
		column++
		chunkCounter++
	}
	return column, chunkCounter
}

func checkBundleChange(handleBundleGroup []int, handleBundleRow int) bool {
	for key := range handleBundleGroup {
		changeNumber := 0
		for i := 0; i <= key; i++ {
			changeNumber += handleBundleGroup[i]
			if changeNumber == handleBundleRow {
				return true
			}
		}
	}
	return false
}

func setHeader(f *excelize.File, kind, title, col string, row, merge int) {
	headerStyle, _ := f.NewStyle(`{"fill":{"type":"pattern","color":["#b1cefc"],"pattern":1},"font":{"bold":true},"alignment":{"wrap_text":true}}`)
	f.NewStyle(headerStyle)
	style, _ := f.NewStyle(`{"fill":{"type":"pattern","color":["#E6E6FA"],"pattern":1},"font":{"bold":true},"alignment":{"wrap_text":true}}`)
	f.NewStyle(style)

	if kind == "transaction" {
		f.SetCellStyle("Sheet1", fmt.Sprintf("%s%d", col, row), fmt.Sprintf("%s%d", col, row), headerStyle)
		f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", col, row), title)
	} else if kind == "puzzle" || kind == "handle-chunks" || kind == "bundle-chunks" {
		colNumber, _ := excelize.ColumnNameToNumber(col)
		colStart, _ := excelize.ColumnNumberToName(colNumber - merge)
		f.MergeCell("Sheet1", fmt.Sprintf("%s%d", colStart, row), fmt.Sprintf("%s%d", col, row))
		if kind == "handle-chunks" {
			f.SetCellStyle("Sheet1", fmt.Sprintf("%s%d", colStart, row), fmt.Sprintf("%s%d", colStart, row), headerStyle)
		} else {
			f.SetCellStyle("Sheet1", fmt.Sprintf("%s%d", colStart, row), fmt.Sprintf("%s%d", colStart, row), style)
		}
		f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", colStart, row), title)
	} else {
		f.SetCellStyle("Sheet1", fmt.Sprintf("%s%d", col, row), fmt.Sprintf("%s%d", col, row), style)
		f.SetCellValue("Sheet1", fmt.Sprintf("%s%d", col, row), title)
		f.SetColWidth("Sheet1", string(col), string(col), 14)
		if kind == "timestamp" {
			f.SetColWidth("Sheet1", string(col), string(col), 28)
		}
	}
}
