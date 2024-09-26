package main

import (
	"github.com/goccy/go-json"
	"math"
	"math/rand"
	"os"
	"time"
)

const OutputFileJsonl = "testdata/data.jsonl"
const OutputFileJson = "testdata/data.json"

type TimeSeriesRow struct {
	Metric    string
	Value     float64
	Labels    map[string]string
	Timestamp int64
}

func (x *TimeSeriesRow) MarshalJSON() ([]byte, error) {
	labelsEncoded, err := json.Marshal(x.Labels)
	if err != nil {
		return nil, err
	}
	data := map[string]interface{}{
		"metric": x.Metric,
		"value":  x.Value,
		"labels": string(labelsEncoded),
		"ts":     x.Timestamp,
	}
	return json.Marshal(data)
}

func genCounterValues(curve func(i int) float64, count int) []float64 {
	values := make([]float64, count)
	values[0] = math.Floor(math.Max(curve(0), 0))
	for i := 1; i < count; i++ {
		values[i] = math.Floor(math.Max(curve(i), 0) + values[i-1])
	}
	return values
}

func main() {
	startTime := time.Date(2024, 9, 18, 0, 0, 0, 0, time.UTC)
	endTime := startTime.Add(24 * time.Hour)
	step := 15 * time.Second
	recordCount := int(endTime.Sub(startTime)/step) + 1
	recordCount = 10

	var rows []TimeSeriesRow

	x := func(i int) float64 {
		return float64(i) / 1000
	}

	tsAt := func(i int) int64 {
		return startTime.Add(time.Duration(i) * step).UnixMilli()
	}

	for i, val := range genCounterValues(func(i int) float64 {
		return (math.Sin(x(i))+math.Cos(math.Sqrt(3)*x(i))+3)*1500 + rand.Float64()*10
	}, recordCount) {
		rows = append(rows,
			TimeSeriesRow{
				Metric:    "http_request_handled",
				Value:     val,
				Labels:    map[string]string{"status": "200", "method": "GET", "path": "/app"},
				Timestamp: tsAt(i) + 1,
			})
	}

	for i, val := range genCounterValues(func(i int) float64 {
		return (math.Sin(x(i))+math.Cos(math.Sqrt(3)*x(i))+3)*10 + rand.Float64()
	}, recordCount) {
		rows = append(rows,
			TimeSeriesRow{
				Metric:    "http_request_handled",
				Value:     val,
				Labels:    map[string]string{"status": "500", "method": "GET", "path": "/app"},
				Timestamp: tsAt(i) + 10,
			})
	}

	for i, val := range genCounterValues(func(i int) float64 {
		return (math.Sin(x(i))+math.Cos(math.Sqrt(3)*x(i))+3)*250 + rand.Float64()*10
	}, recordCount) {
		rows = append(rows,
			TimeSeriesRow{
				Metric:    "http_request_handled",
				Value:     val,
				Labels:    map[string]string{"status": "400", "method": "GET", "path": "/app"},
				Timestamp: tsAt(i) + 100,
			})
	}

	for i, val := range genCounterValues(func(i int) float64 {
		return (math.Sin(x(i))+math.Cos(math.Sqrt(3)*x(i))+3)*2500 + rand.Float64()*10
	}, recordCount) {
		rows = append(rows,
			TimeSeriesRow{
				Metric:    "http_request_handled",
				Value:     val,
				Labels:    map[string]string{"status": "200", "method": "GET", "path": "/api"},
				Timestamp: tsAt(i),
			})
	}

	for i, val := range genCounterValues(func(i int) float64 {
		return (math.Sin(x(i))+math.Cos(math.Sqrt(3)*x(i))+3)*70 + rand.Float64()*10
	}, recordCount) {
		rows = append(rows,
			TimeSeriesRow{
				Metric:    "http_request_handled",
				Value:     val,
				Labels:    map[string]string{"status": "500", "method": "GET", "path": "/api"},
				Timestamp: tsAt(i),
			})
	}

	for i, val := range genCounterValues(func(i int) float64 {
		return (math.Sin(x(i))+math.Cos(math.Sqrt(3)*x(i))+3)*500 + rand.Float64()*10
	}, recordCount) {
		rows = append(rows,
			TimeSeriesRow{
				Metric:    "http_request_handled",
				Value:     val,
				Labels:    map[string]string{"status": "400", "method": "GET", "path": "/api"},
				Timestamp: tsAt(i),
			})
	}

	for i, val := range genCounterValues(func(i int) float64 {
		return (math.Sin(x(i))+math.Cos(math.Sqrt(3)*x(i))+3)*500 + rand.Float64()*10
	}, recordCount) {
		rows = append(rows,
			TimeSeriesRow{
				Metric:    "db_record_write",
				Value:     val,
				Labels:    map[string]string{"db": "app", "table": "user_activity"},
				Timestamp: tsAt(i),
			})
	}

	for i, val := range genCounterValues(func(i int) float64 {
		return (math.Sin(x(i))+math.Cos(math.Sqrt(3)*x(i))+3)*1500 + rand.Float64()*10
	}, recordCount) {
		rows = append(rows,
			TimeSeriesRow{
				Metric:    "db_record_write",
				Value:     val,
				Labels:    map[string]string{"db": "app", "table": "webhook_actions"},
				Timestamp: tsAt(i),
			})
	}

	for i, val := range genCounterValues(func(i int) float64 {
		return (math.Sin(x(i))+math.Cos(math.Sqrt(3)*x(i))+3)*100 + rand.Float64()*10
	}, recordCount) {
		rows = append(rows,
			TimeSeriesRow{
				Metric:    "db_record_write",
				Value:     val,
				Labels:    map[string]string{"db": "app", "table": "background_tasks"},
				Timestamp: tsAt(i),
			})
	}

	{
		file, err := os.OpenFile(OutputFileJsonl, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		for _, row := range rows {
			if err := json.NewEncoder(file).Encode(&row); err != nil {
				panic(err)
			}
		}
	}

	{
		file, err := os.OpenFile(OutputFileJson, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		if err := json.NewEncoder(file).Encode(&rows); err != nil {
			panic(err)
		}
	}

}
