import { DataSourceJsonData } from '@grafana/data';
import { QueryOption } from '../dataquery/QueryOption';

export interface PinotConnectionConfig extends DataSourceJsonData {
  controllerUrl?: string;
  brokerUrl?: string;
  databaseName?: string;
  tokenType?: string;
  queryOptions: QueryOption[];
  oauthPassThru?: boolean;
}

export interface PinotSecureConfig {
  authToken?: string;
}
