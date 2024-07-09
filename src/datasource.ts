import { CoreApp, DataSourceInstanceSettings } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';

import { GetDefaultPinotDataQuery, PinotDataQuery } from './types/PinotDataQuery';
import { PinotConnectionConfig } from './types/PinotConnectionConfig';

export class DataSource extends DataSourceWithBackend<PinotDataQuery, PinotConnectionConfig> {
  constructor(instanceSettings: DataSourceInstanceSettings<PinotConnectionConfig>) {
    super(instanceSettings);
  }

  getDefaultQuery(_: CoreApp): Partial<PinotDataQuery> {
    return GetDefaultPinotDataQuery();
  }
}
