package pinotlib

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParseTimeGroupExpr(t *testing.T) {
	testCases := []struct {
		expr    string
		want    TimeGroupExpression
		wantErr bool
	}{
		{
			expr: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 1, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 1), 1)`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 1, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '2:MINUTES')`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 2, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 2), 2)`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 2, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '5:MINUTES')`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 5, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 5), 5)`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 5, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '10:MINUTES')`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 10, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 10), 10)`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 10, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '15:MINUTES')`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 15, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 15), 15)`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 15, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES')`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 30, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 30), 30)`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 30, Unit: TimeUnitMinutes},
			},
		}, {
			expr: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 1, Unit: TimeUnitHours},
			},
		}, {
			expr: `FromEpochHoursBucket(ToEpochHoursBucket("ts", 1), 1)`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 1, Unit: TimeUnitHours},
			},
		}, {
			expr: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS')`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 1, Unit: TimeUnitDays},
			},
		}, {
			expr: `FromEpochDaysBucket(ToEpochDaysBucket("ts", 1), 1)`,
			want: TimeGroupExpression{
				timeColumn:   "ts",
				inputFormat:  PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				outputFormat: PinotDateTimeFormat{Size: 1, Unit: TimeUnitMilliseconds, Format: TimeFormatEpoch},
				granularity:  PinotGranularity{Size: 1, Unit: TimeUnitDays},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.expr, func(t *testing.T) {
			got, err := ParseTimeGroupExpression(tt.expr)
			assert.Equal(t, tt.want, got)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTimeGroupExpression_Equals(t *testing.T) {
	testArgs := []struct {
		expr1, expr2 string
		want         bool
	}{
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 1), 1)`,
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '2:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 2), 2)`,
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '5:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 5), 5)`,
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '10:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 10), 10)`,
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '15:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 15), 15)`,
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 30), 30)`,
			want:  true,
		},

		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')`,
			expr2: `FromEpochHoursBucket(ToEpochHoursBucket("ts", 1), 1)`,
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS')`,
			expr2: `FromEpochDaysBucket(ToEpochDaysBucket("ts", 1), 1)`,
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 2), 2)`,
			want:  false,
		},
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '2:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 1), 1)`,
			want:  false,
		},
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '5:MINUTES')`,
			expr2: `FromEpochHoursBucket(ToEpochHoursBucket("ts", 5), 5)`,
			want:  false,
		},
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '10:MINUTES')`,
			expr2: `FromEpochDaysBucket(ToEpochDaysBucket("ts", 1), 1)`,
			want:  false,
		},
		{
			expr1: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '15:HOURS')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 15), 15)`,
			want:  false,
		},
	}

	for _, tt := range testArgs {
		t.Run(tt.expr1+"="+tt.expr2, func(t *testing.T) {
			tg1, err := ParseTimeGroupExpression(tt.expr1)
			require.NoError(t, err)
			tg2, err := ParseTimeGroupExpression(tt.expr2)
			require.NoError(t, err)
			assert.Equal(t, tt.want, tg1.Equals(tg2))
		})
	}
}
