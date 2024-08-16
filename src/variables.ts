import { CustomVariableSupport, DataQueryRequest, DataQueryResponse } from '@grafana/data';
import { DataSource } from './datasource';
import { VariableQueryEditor } from './components/VariableQueryEditor/VariableQueryEditor';
import { PinotDataQuery } from './types/PinotDataQuery';
// TODO: The rxjs package doesn't seem to type correctly here.
import { Observable } from '@grafana/data/node_modules/rxjs/dist/types/internal/Observable';
import { assign } from 'lodash';

export class PinotVariableSupport extends CustomVariableSupport<DataSource, PinotDataQuery> {
  constructor(private readonly datasource: DataSource) {
    super();
    this.datasource = datasource;
    this.query = this.query.bind(this);
  }

  editor = VariableQueryEditor;

  query(request: DataQueryRequest<PinotDataQuery>): Observable<DataQueryResponse> {
    assign(request.targets, [{ ...request.targets[0], refId: 'A' }]);

    return this.datasource.query(request);
  }
}
