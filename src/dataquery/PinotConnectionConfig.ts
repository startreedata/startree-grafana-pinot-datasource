import {DataSourceJsonData} from '@grafana/data';
import {QueryOption} from './QueryOption';

/**
 * These are options configured for each DataSource instance
 */
export interface PinotConnectionConfig extends DataSourceJsonData {
  controllerUrl?: string;
  brokerUrl?: string;
  databaseName?: string;
  tokenType?: string;
  queryOptions: QueryOption[];
  oauthPassThru?: boolean;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface PinotSecureConfig {
  authToken?: string;
}
