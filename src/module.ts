import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { ConfigEditor } from './components/ConfigEditor';
import { PinotConnectionConfig } from './types/config';
import {PinotDataQuery} from "./types/PinotDataQuery";
import {QueryEditor} from "./components/QueryEditor";

export const plugin = new DataSourcePlugin<DataSource, PinotDataQuery, PinotConnectionConfig>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
