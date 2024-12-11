import { ComplexField } from '../types/ComplexField';
import { DimensionFilter } from '../types/DimensionFilter';
import { OrderByClause } from '../types/OrderByClause';
import { QueryOption } from '../types/QueryOption';
import { PinotDataQuery } from '../types/PinotDataQuery';
import { AggregationFunction } from '../components/QueryEditor/SelectAggregation';
import { Column } from '../resources/columns';
import { isEmpty } from 'lodash';

export interface BuilderParams {
  tableName: string;
  timeColumn: string;
  metricColumn: ComplexField;
  granularity: string;
  aggregationFunction: string;
  limit: number;
  filters: DimensionFilter[];
  orderBy: OrderByClause[];
  queryOptions: QueryOption[];
  legend: string;
  groupByColumns: ComplexField[];
}

export function builderParamsFrom(query: PinotDataQuery): BuilderParams {
  return {
    tableName: query.tableName || '',
    timeColumn: query.timeColumn || '',
    metricColumn: metricColumnFrom(query) || {},
    granularity: query.granularity || '',
    aggregationFunction: query.aggregationFunction || AggregationFunction.SUM,
    limit: query.limit || 0,
    filters: query.filters || [],
    orderBy: query.orderBy || [],
    queryOptions: query.queryOptions || [],
    legend: query.legend || '',
    groupByColumns: groupByColumnsFrom(query),
  };
}

function groupByColumnsFrom(query: PinotDataQuery): ComplexField[] {
  return [...(query.groupByColumns?.map((col) => ({ name: col })) || []), ...(query.groupByColumnsV2 || [])];
}

function metricColumnFrom(query: PinotDataQuery): ComplexField | undefined {
  if (query.metricColumnV2) {
    return query.metricColumnV2;
  } else if (query.metricColumn) {
    return { name: query.metricColumn };
  } else {
    return undefined;
  }
}

export function canRunBuilderQuery(params: BuilderParams): boolean {
  switch (true) {
    case !params.tableName:
    case !params.timeColumn:
    case !params.metricColumn.name && params.aggregationFunction !== AggregationFunction.COUNT:
      return false;
    default:
      return true;
  }
}

export function applyBuilderDefaults(
  params: BuilderParams,
  resources: {
    timeColumns: Column[];
    metricColumns: Column[];
  }
) {
  if (!params.timeColumn && resources.timeColumns.length > 0) {
    params.timeColumn = resources.timeColumns[0].name;
  }

  if (!params.metricColumn?.name && resources.metricColumns.length > 0) {
    params.metricColumn = { name: resources.metricColumns[0].name, key: resources.metricColumns[0].key || undefined };
  }
}

export function dataQueryWithBuilderParams(query: PinotDataQuery, params: BuilderParams): PinotDataQuery {
  return {
    ...query,
    tableName: params.tableName || undefined,
    timeColumn: params.timeColumn || undefined,
    metricColumnV2: params.metricColumn.name ? params.metricColumn : undefined,
    granularity: params.granularity || undefined,
    aggregationFunction: params.aggregationFunction || undefined,
    limit: params.limit || undefined,
    filters: isEmpty(params.filters) ? undefined : params.filters,
    orderBy: isEmpty(params.orderBy) ? undefined : params.orderBy,
    queryOptions: isEmpty(params.queryOptions) ? undefined : params.queryOptions,
    legend: params.legend || undefined,
    groupByColumnsV2: isEmpty(params.groupByColumns) ? undefined : params.groupByColumns,
  };
}
