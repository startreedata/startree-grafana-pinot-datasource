import { DataSourceInstanceSettings, CoreApp } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';

import { PinotConnectionConfig } from './types/config';
import { PinotQuery, DEFAULT_QUERY } from './types/sql';

export class DataSource extends DataSourceWithBackend<PinotQuery, PinotConnectionConfig> {
  constructor(instanceSettings: DataSourceInstanceSettings<PinotConnectionConfig>) {
    super(instanceSettings);
  }

  getDefaultQuery(_: CoreApp): Partial<PinotQuery> {
    return DEFAULT_QUERY
  }
}
