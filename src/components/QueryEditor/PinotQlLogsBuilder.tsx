import { interpolateVariables, PinotDataQuery } from '../../types/PinotDataQuery';
import { DateTime, ScopedVars } from '@grafana/data';
import { DataSource } from '../../datasource';
import { useColumns } from '../../resources/columns';
import { DisplayTypeLogs, DisplayTypeTimeSeries, SelectDisplayType } from './SelectDisplayType';
import { SelectTable } from './SelectTable';
import { SelectTimeColumn } from './SelectTimeColumn';
import { SelectFilters } from './SelectFilters';
import { SelectQueryOptions } from './SelectQueryOptions';
import { InputLimit } from './InputLimit';
import { SqlPreview } from './SqlPreview';
import React, { useEffect, useState } from 'react';
import { previewLogsSql, PreviewLogsSqlRequest } from '../../resources/previewSql';
import { SelectLogColumn } from './SelectLogColumn';
import { PinotDataType } from '../../types/PinotDataType';
import { SelectJsonExtractors } from './SelectJsonExtractors';
import { SelectMetadataColumns } from './SelectMetadataColumns';
import { SelectRegexpExtractors } from './SelectRegexpExtractors';

export function PinotQlLogsBuilder(props: {
  query: PinotDataQuery;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  datasource: DataSource;
  tables: string[] | undefined;
  scopedVars: ScopedVars;
  onChange: (value: PinotDataQuery) => void;
  onRunQuery: () => void;
}) {
  const { timeRange, tables, intervalSize, datasource, query, scopedVars, onChange, onRunQuery } = props;

  const sqlPreview = useSqlPreview(datasource, intervalSize, timeRange, query, scopedVars);

  const { result: columns, loading: isColumnsLoading } = useColumns(datasource, {
    tableName: query.tableName,
    timeColumn: query.timeColumn,
    timeRange: timeRange,
    filters: query.filters || [],
  });

  function canRunQuery(query: PinotDataQuery) {
    return !!(query.tableName && query.timeColumn && query.logColumn);
  }

  const onChangeAndRun = (newQuery: PinotDataQuery) => {
    onChange(newQuery);
    const interpolated = interpolateVariables(newQuery, scopedVars);
    if (canRunQuery(interpolated)) {
      onRunQuery();
    }
  };

  const timeColumns = columns.filter(({ isTime, isDerived }) => isTime && !isDerived);
  const dimensionColumns = columns.filter(({ isTime }) => !isTime);
  const logColumns = columns.filter(({ dataType }) => dataType === PinotDataType.STRING);

  return (
    <>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <SelectDisplayType
          value={query.displayType}
          displayTypes={[DisplayTypeTimeSeries, DisplayTypeLogs]}
          onChange={(val) => {
            onChange({ ...query, displayType: val });
            onRunQuery();
          }}
        />
        <div className={'gf-form'} data-testid="select-table">
          <SelectTable
            options={tables}
            selected={query.tableName}
            onChange={(value: string | undefined) =>
              onChange({
                ...query,
                tableName: value,
                filters: undefined,
              })
            }
          />
        </div>
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectTimeColumn
          selected={query.timeColumn}
          timeColumns={timeColumns}
          isLoading={isColumnsLoading}
          onChange={(value) => onChangeAndRun({ ...query, timeColumn: value })}
        />
        <SelectLogColumn
          selected={query.logColumn}
          columns={logColumns}
          isLoading={isColumnsLoading}
          onChange={(logColumn) => onChangeAndRun({ ...query, logColumn })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectMetadataColumns
          selected={query.metadataColumns}
          columns={columns.filter(
            ({ name, key }) =>
              query.timeColumn !== name && (query.logColumn?.name !== name || query.logColumn?.key !== key)
          )}
          isLoading={isColumnsLoading}
          onChange={(metadataColumns) => onChangeAndRun({ ...query, metadataColumns })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectJsonExtractors
          extractors={query.jsonExtractors || []}
          columns={dimensionColumns}
          isLoadingColumns={isColumnsLoading}
          onChange={(jsonExtractors) => onChangeAndRun({ ...query, jsonExtractors })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectRegexpExtractors
          extractors={query.regexpExtractors || []}
          columns={dimensionColumns}
          isLoadingColumns={isColumnsLoading}
          onChange={(regexpExtractors) => onChangeAndRun({ ...query, regexpExtractors })}
        />
      </div>
      <div>
        <SelectFilters
          datasource={datasource}
          tableName={query.tableName}
          timeColumn={query.timeColumn}
          timeRange={timeRange}
          dimensionColumns={dimensionColumns}
          dimensionFilters={query.filters || []}
          onChange={(val) => onChangeAndRun({ ...query, filters: val })}
        />
      </div>
      <div>
        <SelectQueryOptions
          selected={query.queryOptions || []}
          onChange={(queryOptions) => onChangeAndRun({ ...query, queryOptions })}
        />
      </div>
      <div>
        <InputLimit current={query.limit} onChange={(limit) => onChangeAndRun({ ...query, limit })} />
      </div>
      <div>
        <SqlPreview sql={sqlPreview} />
      </div>
    </>
  );
}

function useSqlPreview(
  datasource: DataSource,
  intervalSize: string | undefined,
  timeRange: {
    to: DateTime | undefined;
    from: DateTime | undefined;
  },
  query: PinotDataQuery,
  scopedVars: ScopedVars
): string {
  const [sqlPreview, setSqlPreview] = useState('');

  const interpolated = interpolateVariables(query, scopedVars);
  const previewRequest: PreviewLogsSqlRequest = {
    timeRange: {
      to: timeRange.to?.endOf('second'),
      from: timeRange.from?.startOf('second'),
    },
    expandMacros: true,
    tableName: interpolated.tableName,
    timeColumn: interpolated.timeColumn,
    limit: interpolated.limit,
    queryOptions: interpolated.queryOptions,
    logColumn: interpolated.logColumn,
    logColumnAlias: interpolated.logColumnAlias,
    metadataColumns: interpolated.metadataColumns,
    jsonExtractors: interpolated.jsonExtractors,
    regexpExtractors: interpolated.regexpExtractors,
    dimensionFilters: interpolated.filters,
  };

  useEffect(() => {
    previewLogsSql(datasource, previewRequest).then((val) => val && setSqlPreview(val));
  }, [datasource, JSON.stringify(previewRequest)]); // eslint-disable-line react-hooks/exhaustive-deps
  return sqlPreview;
}
