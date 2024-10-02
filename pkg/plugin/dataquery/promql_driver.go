package dataquery

import (
	"bytes"
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana/pkg/promlib/converter"
	jsoniter "github.com/json-iterator/go"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"io"
	"net/http"
	"strings"
	"time"
)

var _ Driver = &PromQlDriver{}

type PromQlCodeDriverParams struct {
	PinotClient *pinotlib.PinotClient

	TableName    string
	PromQlCode   string
	TimeRange    TimeRange
	IntervalSize time.Duration
	Legend       string
}

type PromQlDriver struct {
	params PromQlCodeDriverParams
}

func NewPromQlCodeDriver(params PromQlCodeDriverParams) *PromQlDriver {
	return &PromQlDriver{
		params: params,
	}
}

func (p *PromQlDriver) Execute(ctx context.Context) backend.DataResponse {
	if strings.TrimSpace(p.params.PromQlCode) == "" {
		return backend.DataResponse{}
	}

	queryResponse, err := p.params.PinotClient.ExecuteTimeSeriesQuery(ctx, &pinotlib.TimeSeriesRangeQuery{
		Language:  pinotlib.TimeSeriesQueryLanguagePromQl,
		Query:     p.params.PromQlCode,
		Start:     p.params.TimeRange.From,
		End:       p.params.TimeRange.To,
		Step:      p.params.IntervalSize,
		TableName: p.params.TableName,
	})
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}
	defer queryResponse.Body.Close()

	if queryResponse.StatusCode != http.StatusOK {
		var body bytes.Buffer
		_, _ = body.ReadFrom(queryResponse.Body)
		return NewDataInternalErrorResponse(fmt.Errorf("error executing promql query: %s", body.String()))
	}

	var buf bytes.Buffer
	io.Copy(&buf, queryResponse.Body)

	fmt.Println(buf.String())

	iter := jsoniter.Parse(jsoniter.ConfigDefault, &buf, 1024)
	dataResponse := converter.ReadPrometheusStyleResult(iter, converter.Options{})

	for _, frame := range dataResponse.Frames {
		p.decorateFrame(frame)
	}
	return dataResponse
}

func (p *PromQlDriver) decorateFrame(frame *data.Frame) {
	if len(frame.Fields) < 1 {
		return
	}
	frame.Fields[0].Config = &data.FieldConfig{Interval: float64(p.params.IntervalSize.Milliseconds())}

	for i := 1; i < len(frame.Fields); i++ {
		name := FormatSeriesName(p.params.Legend, frame.Fields[i].Labels)
		frame.Fields[i].SetConfig(&data.FieldConfig{DisplayNameFromDS: name})
	}
}

func newDemoReader() io.Reader {
	z := `{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "metric=http_request_handled",
          "metric": "http_request_handled"
        },
        "values": [
          [
            1726617601,
            null
          ],
          [
            1726617602,
            null
          ],
          [
            1726617603,
            null
          ],
          [
            1726617604,
            null
          ],
          [
            1726617605,
            null
          ],
          [
            1726617606,
            null
          ],
          [
            1726617607,
            null
          ],
          [
            1726617608,
            null
          ],
          [
            1726617609,
            null
          ],
          [
            1726617610,
            null
          ],
          [
            1726617611,
            null
          ],
          [
            1726617612,
            null
          ],
          [
            1726617613,
            null
          ],
          [
            1726617614,
            null
          ],
          [
            1726617615,
            "4012.0"
          ],
          [
            1726617616,
            null
          ],
          [
            1726617617,
            null
          ],
          [
            1726617618,
            null
          ],
          [
            1726617619,
            null
          ],
          [
            1726617620,
            null
          ],
          [
            1726617621,
            null
          ],
          [
            1726617622,
            null
          ],
          [
            1726617623,
            null
          ],
          [
            1726617624,
            null
          ],
          [
            1726617625,
            null
          ],
          [
            1726617626,
            null
          ],
          [
            1726617627,
            null
          ],
          [
            1726617628,
            null
          ],
          [
            1726617629,
            null
          ],
          [
            1726617630,
            "6015.0"
          ],
          [
            1726617631,
            null
          ],
          [
            1726617632,
            null
          ],
          [
            1726617633,
            null
          ],
          [
            1726617634,
            null
          ],
          [
            1726617635,
            null
          ],
          [
            1726617636,
            null
          ],
          [
            1726617637,
            null
          ],
          [
            1726617638,
            null
          ],
          [
            1726617639,
            null
          ],
          [
            1726617640,
            null
          ],
          [
            1726617641,
            null
          ],
          [
            1726617642,
            null
          ],
          [
            1726617643,
            null
          ],
          [
            1726617644,
            null
          ],
          [
            1726617645,
            "8019.0"
          ],
          [
            1726617646,
            null
          ],
          [
            1726617647,
            null
          ],
          [
            1726617648,
            null
          ],
          [
            1726617649,
            null
          ],
          [
            1726617650,
            null
          ],
          [
            1726617651,
            null
          ],
          [
            1726617652,
            null
          ],
          [
            1726617653,
            null
          ],
          [
            1726617654,
            null
          ],
          [
            1726617655,
            null
          ],
          [
            1726617656,
            null
          ],
          [
            1726617657,
            null
          ],
          [
            1726617658,
            null
          ],
          [
            1726617659,
            null
          ],
          [
            1726617660,
            "10030.0"
          ],
          [
            1726617661,
            null
          ],
          [
            1726617662,
            null
          ],
          [
            1726617663,
            null
          ],
          [
            1726617664,
            null
          ],
          [
            1726617665,
            null
          ],
          [
            1726617666,
            null
          ],
          [
            1726617667,
            null
          ],
          [
            1726617668,
            null
          ],
          [
            1726617669,
            null
          ],
          [
            1726617670,
            null
          ],
          [
            1726617671,
            null
          ],
          [
            1726617672,
            null
          ],
          [
            1726617673,
            null
          ],
          [
            1726617674,
            null
          ],
          [
            1726617675,
            "12036.0"
          ],
          [
            1726617676,
            null
          ],
          [
            1726617677,
            null
          ],
          [
            1726617678,
            null
          ],
          [
            1726617679,
            null
          ],
          [
            1726617680,
            null
          ],
          [
            1726617681,
            null
          ],
          [
            1726617682,
            null
          ],
          [
            1726617683,
            null
          ],
          [
            1726617684,
            null
          ],
          [
            1726617685,
            null
          ],
          [
            1726617686,
            null
          ],
          [
            1726617687,
            null
          ],
          [
            1726617688,
            null
          ],
          [
            1726617689,
            null
          ],
          [
            1726617690,
            "14043.0"
          ],
          [
            1726617691,
            null
          ],
          [
            1726617692,
            null
          ],
          [
            1726617693,
            null
          ],
          [
            1726617694,
            null
          ],
          [
            1726617695,
            null
          ],
          [
            1726617696,
            null
          ],
          [
            1726617697,
            null
          ],
          [
            1726617698,
            null
          ],
          [
            1726617699,
            null
          ],
          [
            1726617700,
            null
          ],
          [
            1726617701,
            null
          ],
          [
            1726617702,
            null
          ],
          [
            1726617703,
            null
          ],
          [
            1726617704,
            null
          ],
          [
            1726617705,
            "16049.0"
          ],
          [
            1726617706,
            null
          ],
          [
            1726617707,
            null
          ],
          [
            1726617708,
            null
          ],
          [
            1726617709,
            null
          ],
          [
            1726617710,
            null
          ],
          [
            1726617711,
            null
          ],
          [
            1726617712,
            null
          ],
          [
            1726617713,
            null
          ],
          [
            1726617714,
            null
          ],
          [
            1726617715,
            null
          ],
          [
            1726617716,
            null
          ],
          [
            1726617717,
            null
          ],
          [
            1726617718,
            null
          ],
          [
            1726617719,
            null
          ],
          [
            1726617720,
            "18057.0"
          ],
          [
            1726617721,
            null
          ],
          [
            1726617722,
            null
          ],
          [
            1726617723,
            null
          ],
          [
            1726617724,
            null
          ],
          [
            1726617725,
            null
          ],
          [
            1726617726,
            null
          ],
          [
            1726617727,
            null
          ],
          [
            1726617728,
            null
          ],
          [
            1726617729,
            null
          ],
          [
            1726617730,
            null
          ],
          [
            1726617731,
            null
          ],
          [
            1726617732,
            null
          ],
          [
            1726617733,
            null
          ],
          [
            1726617734,
            null
          ]
        ]
      }
    ]
  },
  "errorType": null,
  "error": null
}
`

	return strings.NewReader(z)
}
