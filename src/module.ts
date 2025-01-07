import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { ConfigEditor } from './components/ConfigEditor/ConfigEditor';
import { PinotDataQuery } from './dataquery/PinotDataQuery';
import { QueryEditor } from './components/QueryEditor/QueryEditor';
import { PinotConnectionConfig } from './dataquery/PinotConnectionConfig';

export const plugin = new DataSourcePlugin<DataSource, PinotDataQuery, PinotConnectionConfig>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
