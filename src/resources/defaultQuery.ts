import { DataSource } from '../datasource';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { PinotResourceResponse } from './PinotResourceResponse';

interface DefaultQueryResponse extends Omit<PinotDataQuery, 'refId'> {}

export function fetchDefaultQuery(datasource: DataSource): Promise<DefaultQueryResponse> {
  return datasource
    .getResource<PinotResourceResponse<PinotDataQuery>>('/defaultQuery')
    .then((resp) => resp.result || {});
}
