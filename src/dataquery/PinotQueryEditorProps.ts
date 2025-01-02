import { QueryEditorProps } from '@grafana/data';
import { DataSource } from '../datasource';
import { PinotDataQuery } from './PinotDataQuery';
import { PinotConnectionConfig } from './PinotConnectionConfig';

export type PinotQueryEditorProps = QueryEditorProps<DataSource, PinotDataQuery, PinotConnectionConfig>;
