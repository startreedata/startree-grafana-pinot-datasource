import { DataSource } from '../datasource';
import { useEffect, useState } from 'react';
import { PinotResourceResponse } from './PinotResourceResponse';
import { DateTime } from '@grafana/data';
import { UseResourceResult } from './UseResourceResult';

export function useTimeSeriesTables(datasource: DataSource): UseResourceResult<string[]> {
  const [result, setResult] = useState<string[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

  useEffect(() => {
    setLoading(true);
    listTimeSeriesTables(datasource)
      .then((tables) => setResult(tables))
      .then(() => setLoading(false));
  }, [datasource]);

  return { result, loading };
}

export async function listTimeSeriesTables(datasource: DataSource): Promise<string[]> {
  const endpoint = '/timeseries/tables';

  type ListTimeSeriesTablesResponse = PinotResourceResponse<string[]>;
  return datasource.getResource<ListTimeSeriesTablesResponse>(endpoint).then((resp) => resp.result || []);
}

export interface ListTimeSeriesMetricsRequest {
  tableName: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
}

export async function listTimeSeriesMetrics(
  datasource: DataSource,
  request: ListTimeSeriesMetricsRequest
): Promise<string[]> {
  const endpoint = 'timeseries/metrics';

  type ListTimeSeriesMetricsResponse = PinotResourceResponse<string[]>;
  if (request.tableName && request.timeRange.from && request.timeRange.to) {
    return datasource.postResource<ListTimeSeriesMetricsResponse>(endpoint, request).then((resp) => resp.result || []);
  }
  return [];
}

interface ListTimeSeriesLabelsRequest {
  tableName: string | undefined;
  metricName: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
}

export async function listTimeSeriesLabels(
  datasource: DataSource,
  request: ListTimeSeriesLabelsRequest
): Promise<string[]> {
  const endpoint = 'timeseries/labels';

  if (request.tableName && request.timeRange.from && request.timeRange.to) {
    type ListTimeSeriesLabelsResponse = PinotResourceResponse<string[]>;
    return datasource.postResource<ListTimeSeriesLabelsResponse>(endpoint, request).then((resp) => resp.result || []);
  }
  return [];
}

interface ListTimeSeriesLabelValuesRequest {
  tableName: string | undefined;
  metricName: string | undefined;
  labelName: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
}

export async function listTimeSeriesLabelValues(
  datasource: DataSource,
  request: ListTimeSeriesLabelValuesRequest
): Promise<string[]> {
  const endpoint = 'timeseries/labelValues';

  if (request.tableName && request.labelName && request?.timeRange.from && request?.timeRange.to) {
    type ListTimeSeriesLabelValuesResponse = PinotResourceResponse<string[]>;
    return datasource
      .postResource<ListTimeSeriesLabelValuesResponse>(endpoint, request)
      .then((resp) => resp.result || []);
  }
  return [];
}
