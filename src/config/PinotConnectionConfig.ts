import { DataSourceJsonData } from '@grafana/data';
import { QueryOption } from '../dataquery/QueryOption';

export interface PinotConnectionConfig extends DataSourceJsonData {
  controllerUrl?: string;
  brokerUrl?: string;
  databaseName?: string;
  tokenType?: string;
  queryOptions: QueryOption[];
  oauthPassThru?: boolean;
  queryTimeoutSeconds?: number;
  maxRowLimit?: number;

  // Transport security. These are the standard keys the Grafana SDK HTTP client
  // reads (settings.HTTPClientOptions); the backend honors them automatically.
  // Custom headers are stored as httpHeaderName{N} (jsonData) by InputCustomHeaders,
  // so they aren't typed individually here.
  tlsSkipVerify?: boolean;
  tlsAuth?: boolean;
  tlsAuthWithCACert?: boolean;
  serverName?: string;

  // Trace-to-logs correlation. When all four are set, the traces builder attaches a data link from
  // each span to a logs query against this table, keyed by the span's trace id.
  tracesToLogsTable?: string;
  tracesToLogsTraceIdColumn?: string;
  tracesToLogsTimeColumn?: string;
  tracesToLogsLogColumn?: string;

  // Logs-to-trace correlation (the reverse of the above). When all three are set, the logs path
  // attaches a data link from each log row's trace id to a traces query against this table, keyed
  // by that trace id. The trace id is read back out of the row's `labels` (where the configured
  // trace-id metadata column lands), so traceIdColumn must also be wired in as a logs metadata
  // column for the value to be present.
  logsToTracesTable?: string;
  logsToTracesTraceIdColumn?: string;
  logsToTracesTimeColumn?: string;
}

export interface PinotSecureConfig {
  authToken?: string;

  // Secret transport-security fields read by the Grafana SDK HTTP client.
  // Custom header values are stored as httpHeaderValue{N}.
  tlsCACert?: string;
  tlsClientCert?: string;
  tlsClientKey?: string;
}
