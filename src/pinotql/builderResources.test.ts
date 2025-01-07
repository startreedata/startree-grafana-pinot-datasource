import { BuilderResources, builderResourcesFrom } from './builderResources';
import { UseResourceResult } from '../resources/UseResourceResult';
import { Column } from '../resources/columns';
import { Granularity } from '../resources/granularities';

test('builderResourcesFrom', () => {
  const tablesResult: UseResourceResult<string[]> = { loading: false, result: ['table_1', 'table_2'] };
  const columnsResult: UseResourceResult<Column[]> = {
    loading: false,
    result: [
      {
        name: 'ts',
        dataType: 'TIMESTAMP',
        key: null,
        isTime: true,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'ts2',
        dataType: 'TIMESTAMP',
        key: null,
        isTime: true,
        isDerived: true,
        isMetric: false,
      },
      {
        name: 'met',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'met2',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'dim',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'dim2',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
    ],
  };

  const granularitiesResult: UseResourceResult<Granularity[]> = {
    loading: false,
    result: [{ name: 'SECONDS', optimized: false, seconds: 1 }],
  };

  const sqlPreviewResult: UseResourceResult<string> = {
    loading: false,
    result: 'SELECT * FROM "test_table";',
  };

  const got = builderResourcesFrom(tablesResult, columnsResult, granularitiesResult, sqlPreviewResult);
  expect(got).toEqual<BuilderResources>({
    tables: ['table_1', 'table_2'],
    isTablesLoading: false,
    columns: [
      {
        name: 'ts',
        dataType: 'TIMESTAMP',
        key: null,
        isTime: true,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'ts2',
        dataType: 'TIMESTAMP',
        key: null,
        isTime: true,
        isDerived: true,
        isMetric: false,
      },
      {
        name: 'met',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'met2',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'dim',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'dim2',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
    ],
    timeColumns: [
      {
        name: 'ts',
        dataType: 'TIMESTAMP',
        key: null,
        isTime: true,
        isDerived: false,
        isMetric: false,
      },
    ],
    metricColumns: [
      {
        name: 'met',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'met2',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
    ],
    groupByColumns: [
      {
        name: 'met',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'met2',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'dim',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'dim2',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
    ],
    filterColumns: [
      {
        name: 'met',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'met2',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'dim',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'dim2',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
    ],
    isColumnsLoading: false,
    granularities: [{ name: 'SECONDS', optimized: false, seconds: 1 }],
    isGranularitiesLoading: false,
    sqlPreview: 'SELECT * FROM "test_table";',
    isSqlPreviewLoading: false,
  });
});
