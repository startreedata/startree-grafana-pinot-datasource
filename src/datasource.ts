import { AdHocVariableFilter, DataSourceInstanceSettings, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';

import { interpolateVariables, PinotDataQuery } from './dataquery/PinotDataQuery';
import { PinotConnectionConfig } from './config/PinotConnectionConfig';
import { PinotVariableSupport } from './variables';
import { AnnotationsQueryEditor } from './components/AnnotationsQueryEditor/AnnotationsQueryEditor';

export class DataSource extends DataSourceWithBackend<PinotDataQuery, PinotConnectionConfig> {
  constructor(instanceSettings: DataSourceInstanceSettings<PinotConnectionConfig>) {
    super(instanceSettings);

    this.variables = new PinotVariableSupport(this);
    this.annotations = { QueryEditor: AnnotationsQueryEditor };
  }

  applyTemplateVariables(
    query: PinotDataQuery,
    scopedVars: ScopedVars,
    filters?: AdHocVariableFilter[]
  ): PinotDataQuery {
    return interpolateVariables(query, scopedVars);
  }

  interpolateVariablesInQueries(
    queries: PinotDataQuery[],
    scopedVars: ScopedVars,
    filters?: AdHocVariableFilter[]
  ): PinotDataQuery[] {
    return queries.map((query) => interpolateVariables(query, scopedVars));
  }
}
