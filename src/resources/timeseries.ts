import { DataSource } from '../datasource';
import { useEffect, useState } from 'react';
import { PinotResourceResponse } from './PinotResourceResponse';
import { DateTime } from '@grafana/data';

export function useTimeSeriesTables(datasource: DataSource): string[] | undefined {
  const [tables, setTables] = useState<string[] | undefined>();

  useEffect(() => {
    listTimeSeriesTables(datasource).then((tables) => setTables(tables));
  }, [datasource]);

  return tables;
}

interface ListTimeSeriesTablesResponse extends PinotResourceResponse {
  tables: string[] | null;
}

export async function listTimeSeriesTables(datasource: DataSource): Promise<string[]> {
  const endpoint = '/timeseries/tables';

  return datasource.getResource<ListTimeSeriesTablesResponse>(endpoint).then((resp) => resp.tables || []);
}

export interface ListTimeSeriesMetricsRequest {
  tableName: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
}

interface ListTimeSeriesMetricsResponse extends PinotResourceResponse {
  metrics: string[] | null;
}

export async function listTimeSeriesMetrics(
  datasource: DataSource,
  request: ListTimeSeriesMetricsRequest
): Promise<string[]> {
  const endpoint = 'timeseries/metrics';

  console.log({request});

  if (request.tableName && request.timeRange.from && request.timeRange.to) {
    return datasource.postResource<ListTimeSeriesMetricsResponse>(endpoint, request).then((resp) => resp.metrics || []);
  }
  return [];
}

interface ListTimeSeriesLabelsRequest {
  tableName: string | undefined;
  metricName: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
}

interface ListTimeSeriesLabelsResponse extends PinotResourceResponse {
  labels: string[] | null;
}

export async function listTimeSeriesLabels(
  datasource: DataSource,
  request: ListTimeSeriesLabelsRequest
): Promise<string[]> {
  const endpoint = 'timeseries/labels';

  if (request.tableName && request.timeRange.from && request.timeRange.to) {
    return datasource.postResource<ListTimeSeriesLabelsResponse>(endpoint, request).then((resp) => resp.labels || []);
  }
  return [];
}

interface ListTimeSeriesLabelValuesRequest {
  tableName: string | undefined;
  metricName: string | undefined;
  labelName: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
}

interface ListTimeSeriesLabelValuesResponse extends PinotResourceResponse {
  labelValues: string[] | null;
}

export async function listTimeSeriesLabelValues(
  datasource: DataSource,
  request: ListTimeSeriesLabelValuesRequest
): Promise<string[]> {
  const endpoint = 'timeseries/labelValues';

  if (request.tableName && request.labelName && request?.timeRange.from && request?.timeRange.to) {
    return datasource
      .postResource<ListTimeSeriesLabelValuesResponse>(endpoint, request)
      .then((resp) => resp.labelValues || []);
  }
  return [];
}

export interface LabelAndValues {
  name: string;
  values: string[];
}

interface GetTimeSeriesMetricLabelsCollectionRequest {
  tableName: string | undefined;
  metricName: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
}

interface GetTimeSeriesMetricLabelsCollectionResponse extends PinotResourceResponse {
  collection: LabelAndValues[] | null;
}

export async function getTimeSeriesMetricLabelsCollection(
  datasource: DataSource,
  request: GetTimeSeriesMetricLabelsCollectionRequest
): Promise<LabelAndValues[]> {
  const endpoint = 'timeseries/metricLabelsCollection';
  if (request.tableName && request.metricName && request?.timeRange.from && request?.timeRange.to) {
    return datasource
      .postResource<GetTimeSeriesMetricLabelsCollectionResponse>(endpoint, request)
      .then((resp) => resp.collection || []);
  }
  return [];
}
