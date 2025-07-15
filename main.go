package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

type InputData struct {
	BatchType   string `json:"batchType"`
	CreateTime  string `json:"createTime"`
	Creator     string `json:"creator"`
	Labels      struct {
		Location string `json:"goog-dataproc-location"`
	} `json:"labels"`
	RuntimeConfig struct {
		Properties map[string]string `json:"properties"`
	} `json:"runtimeConfig"`
	State       string `json:"state"`
	StateTime   string `json:"stateTime"`
	StateHistory []struct {
		State      string `json:"state"`
		StateStartTime string `json:"stateStartTime"`
	} `json:"stateHistory"`
	RuntimeInfo struct {
		ApproximateUsage struct {
			AcceleratorType       string `json:"acceleratorType"`
			MilliDcuSeconds       string `json:"milliDcuSeconds"`
			ShuffleStorageGbSeconds string `json:"shuffleStorageGbSeconds"`
		} `json:"approximateUsage"`
	} `json:"runtimeInfo"`
}

type OutputData struct {
	BatchType             string            `json:"batchType"`
	CreateTime            string            `json:"createTime"`
	Creator               string            `json:"creator"`
	GoogDataprocLocation  string            `json:"goog-dataproc-location"`
	Properties            map[string]string `json:"properties"`
	State                 string            `json:"state"`
	AcceleratorType       string            `json:"acceleratorType"`
	MilliDcuSeconds       string            `json:"milliDcuSeconds"`
	ShuffleStorageGbSeconds string            `json:"shuffleStorageGbSeconds"`
	ElapsedTime           string            `json:"elapsed_time"`
	RunTime               string            `json:"run_time"`
}

func main() {
	inputFile := flag.String("input", "", "Input JSON file")
	outputFile := flag.String("output", "", "Output JSON file")
	flag.Parse()

	if *inputFile == "" || *outputFile == "" {
		fmt.Println("Both --input and --output flags are required.")
		os.Exit(1)
	}

	jsonFile, err := os.Open(*inputFile)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer jsonFile.Close()

	outputF, err := os.Create(*outputFile)
	if err != nil {
		fmt.Println("error creating output file:", err)
		return
	}
	defer outputF.Close()

	decoder := json.NewDecoder(jsonFile)

	// Read the opening bracket
	_, err = decoder.Token()
	if err != nil {
		fmt.Printf("error reading opening bracket: %v\n", err)
		return
	}

	// Process each object in the array
	for decoder.More() {
		var item InputData
		err := decoder.Decode(&item)
		if err != nil {
			fmt.Printf("error decoding item: %v\n", err)
			return
		}

		if item.BatchType == "" || item.CreateTime == "" || item.Creator == "" || item.Labels.Location == "" || item.State == "" {
			continue
		}

		// Sanitize property keys
		sanitizedProperties := make(map[string]string)
		for key, value := range item.RuntimeConfig.Properties {
			sanitizedKey := strings.ReplaceAll(key, ":", "_")
			sanitizedKey = strings.ReplaceAll(sanitizedKey, ".", "_")
			sanitizedProperties[sanitizedKey] = value
		}

		// Calculate elapsed time
		createTime, err := time.Parse(time.RFC3339, item.CreateTime)
		if err != nil {
			fmt.Printf("error parsing create time: %v\n", err)
			continue
		}
		stateTime, err := time.Parse(time.RFC3339, item.StateTime)
		if err != nil {
			fmt.Printf("error parsing state time: %v\n", err)
			continue
		}
		elapsedTime := math.Round(stateTime.Sub(createTime).Seconds())

		// Calculate run time
		var runTime float64
		for _, history := range item.StateHistory {
			if history.State == "RUNNING" {
				runningTime, err := time.Parse(time.RFC3339, history.StateStartTime)
				if err != nil {
					fmt.Printf("error parsing running time: %v\n", err)
					continue
				}
				runTime = math.Round(stateTime.Sub(runningTime).Seconds())
				break
			}
		}

		output := OutputData{
			BatchType:             item.BatchType,
			CreateTime:            item.CreateTime,
			Creator:               item.Creator,
			GoogDataprocLocation:  item.Labels.Location,
			Properties:            sanitizedProperties,
			State:                 item.State,
			AcceleratorType:       item.RuntimeInfo.ApproximateUsage.AcceleratorType,
			MilliDcuSeconds:       item.RuntimeInfo.ApproximateUsage.MilliDcuSeconds,
			ShuffleStorageGbSeconds: item.RuntimeInfo.ApproximateUsage.ShuffleStorageGbSeconds,
			ElapsedTime:           fmt.Sprintf("%.0f", elapsedTime),
			RunTime:               fmt.Sprintf("%.0f", runTime),
		}

		outputBytes, err := json.Marshal(output)
		if err != nil {
			fmt.Printf("error marshalling output: %v\n", err)
			continue
		}

		fmt.Fprintln(outputF, string(outputBytes))
	}

	// Read the closing bracket
	_, err = decoder.Token()
	if err != nil && err != io.EOF {
		fmt.Printf("error reading closing bracket: %v\n", err)
	}
}