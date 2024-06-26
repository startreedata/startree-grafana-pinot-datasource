import { CoreApp, DataSourceInstanceSettings } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';

import { PinotConnectionConfig } from './types/config';
import { GetDefaultPinotDataQuery, PinotDataQuery } from './types/PinotDataQuery';

export class DataSource extends DataSourceWithBackend<PinotDataQuery, PinotConnectionConfig> {
  constructor(instanceSettings: DataSourceInstanceSettings<PinotConnectionConfig>) {
    super(instanceSettings);
  }

  getDefaultQuery(_: CoreApp): Partial<PinotDataQuery> {
    return GetDefaultPinotDataQuery();
  }
}
