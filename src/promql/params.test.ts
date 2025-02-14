import { dataQueryOf, Params, paramsFrom } from './params';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';

describe('paramsFrom', () => {
  test('query is empty', () => {
    expect(paramsFrom({ refId: 'test_id' })).toEqual<Params>({
      tableName: '',
      promQlCode: '',
      legend: '',
    });
  });

  test('query is fully populated', () => {
    expect(
      paramsFrom({
        refId: 'test_id',
        tableName: 'test_table',
        promQlCode: 'sum(rate(http_requests[15m])) by(path)',
        legend: '{{path}}',
      })
    ).toEqual<Params>({
      tableName: 'test_table',
      promQlCode: 'sum(rate(http_requests[15m])) by(path)',
      legend: '{{path}}',
    });
  });
});

describe('dataQueryWithParams', () => {
  const query = { refId: 'test_id' };
  test('params are empty', () => {
    expect(
      dataQueryOf(query, {
        tableName: '',
        promQlCode: '',
        legend: '',
      })
    ).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PromQL,
      tableName: undefined,
      promQlCode: undefined,
      legend: undefined,
    });
  });

  test('params are fully populated', () => {
    expect(
      dataQueryOf(query, {
        tableName: 'test_table',
        promQlCode: 'sum(rate(http_requests[15m])) by(path)',
        legend: '{{path}}',
      })
    ).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PromQL,
      tableName: 'test_table',
      promQlCode: 'sum(rate(http_requests[15m])) by(path)',
      legend: '{{path}}',
    });
  });
});
