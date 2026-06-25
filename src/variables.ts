import { CustomVariableSupport, DataQueryRequest, DataQueryResponse } from '@grafana/data';
import { DataSource } from './datasource';
import { VariableQueryEditor } from './components/VariableQueryEditor/VariableQueryEditor';
import { PinotDataQuery } from './dataquery/PinotDataQuery';
import { escapeSqlString } from './utils/subquery.util';
// TODO: The rxjs package doesn't seem to type correctly here.
import { Observable } from '@grafana/data/node_modules/rxjs/dist/types/internal/Observable';
import { assign } from 'lodash';

// Turns the variable dropdown's typeahead text into the value substituted for the $__searchFilter
// macro. Follows Grafana's SQL convention: escape quotes, append a trailing `%` so an author's
// `LIKE '$__searchFilter'` does prefix matching, and default to `%` (match-all) when nothing is
// typed so the query stays valid. Exported for testing.
export function searchFilterLikeValue(typed: string): string {
  return typed ? `${escapeSqlString(typed)}%` : '%';
}

// Grafana injects the typeahead text as scopedVars.__searchFilter (raw, no wildcard). Normalize it
// to the LIKE pattern above before the query is interpolated and sent to the backend. Exported for
// testing.
export function withSearchFilter(request: DataQueryRequest<PinotDataQuery>): DataQueryRequest<PinotDataQuery> {
  const current = request.scopedVars?.__searchFilter?.value;
  const typed = typeof current === 'string' ? current : '';
  return {
    ...request,
    scopedVars: { ...request.scopedVars, __searchFilter: { text: typed, value: searchFilterLikeValue(typed) } },
  };
}

export class PinotVariableSupport extends CustomVariableSupport<DataSource, PinotDataQuery> {
  constructor(private readonly datasource: DataSource) {
    super();
    this.datasource = datasource;
    this.query = this.query.bind(this);
  }

  editor = VariableQueryEditor;

  query(request: DataQueryRequest<PinotDataQuery>): Observable<DataQueryResponse> {
    assign(request.targets, [{ ...request.targets[0], refId: 'A' }]);

    return this.datasource.query(withSearchFilter(request));
  }
}
