import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { ConfigEditor } from './components/ConfigEditor';
import { QueryEditor } from './components/QueryEditor';
import { PinotConnectionConfig } from './types/config';
import { PinotQuery } from './types/sql';

export const plugin = new DataSourcePlugin<DataSource, PinotQuery, PinotConnectionConfig>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
