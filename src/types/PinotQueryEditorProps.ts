import { QueryEditorProps } from '@grafana/data';
import { DataSource } from '../datasource';
import { PinotConnectionConfig } from './config';
import {PinotDataQuery} from "./PinotDataQuery";

export type PinotQueryEditorProps = QueryEditorProps<DataSource, PinotDataQuery, PinotConnectionConfig>;
