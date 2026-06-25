package dataquery

import (
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"sort"
)

// Fixed result-column aliases the trace SQL projects into, and that BuildTracesDataFrame reads back
// out. These are also the field names Grafana's trace view expects in the DataFrame.
const (
	TraceFieldTraceID       = "traceID"
	TraceFieldSpanID        = "spanID"
	TraceFieldParentSpanID  = "parentSpanID"
	TraceFieldServiceName   = "serviceName"
	TraceFieldOperationName = "operationName"
	TraceFieldStartTime     = "startTime"
	TraceFieldDuration      = "duration"
	TraceFieldTags          = "tags"
	TraceFieldStatusCode    = "statusCode"
)

// TraceToLogsLink fully describes the internal data link attached to each span's traceID, so that
// clicking a span opens a logs query for that trace. It is non-nil only when the datasource has a
// fully configured trace-to-logs mapping.
type TraceToLogsLink struct {
	DatasourceUID  string
	DatasourceName string
	LogsTable      string
	TraceIdColumn  string
	TimeColumn     string
	LogColumn      string
}

// traceKeyValue is one span tag/attribute, encoded the way Grafana's trace view expects.
type traceKeyValue struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// BuildTracesDataFrame converts a Pinot span result (one row per span, columns aliased to the fixed
// trace field names above) into Grafana's trace DataFrame. startTime is emitted as epoch
// milliseconds and duration in milliseconds (the raw duration column scaled by durationFactor);
// any tags column is decoded into a per-span array of {key, value}. The frame is flagged as a
// trace visualization. When link is non-nil, an internal data link from each span's traceID to a
// logs query for that trace is attached. The same builder serves both find-by-ID and search modes
// (they differ only in the SQL, not the frame shape).
func BuildTracesDataFrame(results *pinot.ResultTable, startTimeFormat pinot.DateTimeFormat, durationFactor float64, link *TraceToLogsLink) (*data.Frame, error) {
	rowCount := results.RowCount()

	traceIDs, err := requiredStringField(results, TraceFieldTraceID)
	if err != nil {
		return nil, err
	}
	spanIDs, err := requiredStringField(results, TraceFieldSpanID)
	if err != nil {
		return nil, err
	}

	startIdx, err := pinot.GetColumnIdx(results, TraceFieldStartTime)
	if err != nil {
		return nil, fmt.Errorf("could not extract start time column: %w", err)
	}
	startTimes, err := pinot.ExtractColumnAsTime(results, startIdx, startTimeFormat)
	if err != nil {
		return nil, fmt.Errorf("could not extract start time column: %w", err)
	}
	startMillis := make([]float64, rowCount)
	for i, t := range startTimes {
		startMillis[i] = float64(t.UnixMilli())
	}

	durationIdx, err := pinot.GetColumnIdx(results, TraceFieldDuration)
	if err != nil {
		return nil, fmt.Errorf("could not extract duration column: %w", err)
	}
	durations, err := pinot.ExtractColumnAsDoubles(results, durationIdx)
	if err != nil {
		return nil, fmt.Errorf("could not extract duration column: %w", err)
	}
	durationMillis := make([]float64, rowCount)
	for i, d := range durations {
		durationMillis[i] = d * durationFactor
	}

	traceIDField := data.NewField(TraceFieldTraceID, nil, traceIDs)
	if link != nil {
		traceIDField.Config = &data.FieldConfig{Links: []data.DataLink{traceToLogsDataLink(*link)}}
	}

	fields := data.Fields{
		traceIDField,
		data.NewField(TraceFieldSpanID, nil, spanIDs),
		data.NewField(TraceFieldStartTime, nil, startMillis),
		data.NewField(TraceFieldDuration, nil, durationMillis),
	}

	// Optional span columns: present only when the user mapped them in the builder.
	for _, name := range []string{TraceFieldParentSpanID, TraceFieldServiceName, TraceFieldOperationName, TraceFieldStatusCode} {
		if values, ok, err := optionalStringField(results, name); err != nil {
			return nil, err
		} else if ok {
			fields = append(fields, data.NewField(name, nil, values))
		}
	}

	if tagsIdx, err := pinot.GetColumnIdx(results, TraceFieldTags); err == nil {
		tags, err := extractSpanTags(results, tagsIdx)
		if err != nil {
			return nil, fmt.Errorf("could not extract tags column: %w", err)
		}
		fields = append(fields, data.NewField(TraceFieldTags, nil, tags))
	}

	frame := data.NewFrame("traces", fields...)
	frame.Meta = &data.FrameMeta{PreferredVisualization: data.VisTypeTrace}
	return frame, nil
}

func requiredStringField(results *pinot.ResultTable, name string) ([]string, error) {
	idx, err := pinot.GetColumnIdx(results, name)
	if err != nil {
		return nil, fmt.Errorf("could not extract %s column: %w", name, err)
	}
	values, err := pinot.ExtractColumnAsStrings(results, idx)
	if err != nil {
		return nil, fmt.Errorf("could not extract %s column: %w", name, err)
	}
	return values, nil
}

func optionalStringField(results *pinot.ResultTable, name string) ([]string, bool, error) {
	idx, err := pinot.GetColumnIdx(results, name)
	if err != nil {
		return nil, false, nil //nolint:nilerr // absent optional column is not an error
	}
	values, err := pinot.ExtractColumnAsStrings(results, idx)
	if err != nil {
		return nil, false, fmt.Errorf("could not extract %s column: %w", name, err)
	}
	return values, true, nil
}

// extractSpanTags decodes a JSON/MAP/STRING tags column into one {key,value} array per span,
// emitted as JSON so Grafana's trace view renders them as span tags. Keys are sorted for stable
// output.
func extractSpanTags(results *pinot.ResultTable, colIdx int) ([]json.RawMessage, error) {
	var maps []map[string]any
	if results.DataSchema.ColumnDataTypes[colIdx] == pinot.DataTypeMap {
		col, err := pinot.ExtractColumn(results, colIdx)
		if err != nil {
			return nil, err
		}
		var ok bool
		if maps, ok = col.([]map[string]any); !ok {
			return nil, fmt.Errorf("tags column is not a map column")
		}
	} else {
		var err error
		if maps, err = pinot.DecodeJsonFromColumn[map[string]any](results, colIdx); err != nil {
			return nil, err
		}
	}

	out := make([]json.RawMessage, len(maps))
	for i, m := range maps {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		pairs := make([]traceKeyValue, len(keys))
		for j, k := range keys {
			pairs[j] = traceKeyValue{Key: k, Value: m[k]}
		}
		encoded, err := json.Marshal(pairs)
		if err != nil {
			return nil, err
		}
		out[i] = encoded
	}
	return out, nil
}

// traceToLogsDataLink builds an internal data link from a span to a PinotQL logs query against the
// configured logs table, filtered by the span's trace id (`${__value.raw}` resolves to the
// traceID field's value for the clicked span).
func traceToLogsDataLink(link TraceToLogsLink) data.DataLink {
	query := map[string]any{
		"queryType":   string(QueryTypePinotQl),
		"editorMode":  string(EditorModeBuilder),
		"displayType": string(DisplayTypeLogs),
		"tableName":   link.LogsTable,
		"timeColumn":  link.TimeColumn,
		"logColumn":   map[string]any{"name": link.LogColumn},
		"filters": []map[string]any{
			{
				"columnName": link.TraceIdColumn,
				"operator":   "=",
				"valueExprs": []string{"'${__value.raw}'"},
			},
		},
	}
	return data.DataLink{
		Title: "Logs for this trace",
		Internal: &data.InternalDataLink{
			Query:          query,
			DatasourceUID:  link.DatasourceUID,
			DatasourceName: link.DatasourceName,
		},
	}
}
