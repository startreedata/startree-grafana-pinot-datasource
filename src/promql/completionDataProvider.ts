import { Label } from './situation';
import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { useMemo } from 'react';
import { listTimeSeriesLabels, listTimeSeriesLabelValues, listTimeSeriesMetrics } from '../resources/timeseries';

export interface CompletionDataProvider {
  getMetricNames: () => Promise<string[]>;
  getLabelsFor: (metricName: string | undefined, otherLabels?: Label[]) => Promise<string[] | undefined>;
  getLabelValuesFor: (metricName: string | undefined, labelName: string, otherLabels?: Label[]) => Promise<string[]>;
}

export function useCompletionDataProvider(
  datasource: DataSource,
  tableName: string | undefined,
  timeRange: {
    to: DateTime | undefined;
    from: DateTime | undefined;
  }
): CompletionDataProvider {
  const cache = useMemo(
    () => new Map<string, Promise<string[]>>(),
    [datasource, tableName, timeRange.to, timeRange.from] // eslint-disable-line
  );

  const retrieve = (key: string | undefined, generator: () => Promise<string[]>): Promise<string[]> => {
    if (!tableName || !key) {
      return new Promise(() => []);
    }

    if (!cache.has(key)) {
      cache.set(key, generator());
    }
    return cache.get(key) || new Promise(() => []);
  };

  const getMetricNames = () =>
    retrieve('kind=metrics', () =>
      listTimeSeriesMetrics(datasource, {
        tableName: tableName,
        timeRange: { to: timeRange.to, from: timeRange.from },
      })
    );

  const getLabelsFor = (metricName: string | undefined, otherLabels: Label[] | undefined): Promise<string[]> =>
    retrieve(`kind=labels&metric=${metricName}`, () =>
      listTimeSeriesLabels(datasource, {
        tableName: tableName,
        metricName: metricName,
        timeRange: { to: timeRange.to, from: timeRange.from },
      })
    );

  const getLabelValuesFor = (
    metricName: string | undefined,
    labelName: string,
    otherLabels: Label[] | undefined
  ): Promise<string[]> =>
    retrieve(`kind=labelValues&metric=${metricName}&label=${labelName}`, () =>
      listTimeSeriesLabelValues(datasource, {
        tableName: tableName,
        metricName: metricName,
        labelName: labelName,
        timeRange: { to: timeRange.to, from: timeRange.from },
      })
    );

  return {
    getMetricNames,
    getLabelsFor,
    getLabelValuesFor,
  };
}
