import {DataSourcePlugin} from '@grafana/data';
import {DataSource} from './datasource';
import {ConfigEditor} from './components/ConfigEditor';
import {PinotConnectionConfig} from './types/config';
import {PinotQuery} from './types/sql';
import {TimeSeriesQueryEditor} from "./components/TimeSeriesEditor";

export const plugin = new DataSourcePlugin<DataSource, PinotQuery, PinotConnectionConfig>(DataSource)
.setConfigEditor(ConfigEditor)
.setQueryEditor(TimeSeriesQueryEditor);
