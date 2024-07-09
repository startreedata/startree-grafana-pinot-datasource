import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { ConfigEditor } from './components/ConfigEditor';
import { PinotDataQuery } from './types/PinotDataQuery';
import { QueryEditor } from './components/QueryEditor';
import { PinotConnectionConfig } from './types/PinotConnectionConfig';

export const plugin = new DataSourcePlugin<DataSource, PinotDataQuery, PinotConnectionConfig>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
