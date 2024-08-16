import React, { useEffect, useState } from 'react';
import { PinotDataQuery } from '../../types/PinotDataQuery';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from '../../datasource';
import { PinotConnectionConfig } from '../../types/PinotConnectionConfig';
import { QueryType } from '../../types/QueryType';
import { useTables, useTableSchema } from '../../resources/controller';
import { SelectTable } from './SelectTable';
import { SelectVariableType, VariableType } from './SelectVariableType';
import { PinotVariableQuery } from '../../types/PinotVariableQuery';
import { DistinctValuesVariableEditor } from './DistinctValuesVariableEditor';
import { SqlVariableEditor } from './SqlVariableEditor';
import { fetchDistinctValuesSqlPreview } from '../../resources/distinctValues';
import { ColumnTypes } from './SelectColumnType';
import { ColumnVariableEditor } from './ColumnVariableEditor';

type VariableQueryEditorProps = QueryEditorProps<DataSource, PinotDataQuery, PinotConnectionConfig, PinotDataQuery>;

export function VariableQueryEditor({ datasource, onChange, query }: VariableQueryEditorProps) {
  const onChangeVariableQuery = (variableQuery: PinotVariableQuery) => {
    onChange({ ...query, variableQuery });
  };

  const tables = useTables(datasource);
  const tableSchema = useTableSchema(datasource, query.tableName);

  const columns = [
    ...(tableSchema?.dateTimeFieldSpecs || []),
    ...(tableSchema?.metricFieldSpecs || []),
    ...(tableSchema?.dimensionFieldSpecs || []),
  ]
    .map(({ name }) => name)
    .sort();

  if (
    query.queryType !== QueryType.PinotVariableQuery ||
    query.variableQuery?.variableType === undefined ||
    query.variableQuery?.columnType === undefined
  ) {
    onChange({
      ...query,
      queryType: QueryType.PinotVariableQuery,
      variableQuery: {
        ...query.variableQuery,
        variableType: VariableType.TableList,
        columnType: ColumnTypes.All,
      },
    });
  }

  const [distinctValuesSqlPreview, setDistinctValuesSqlPreview] = useState<string>('');
  useEffect(() => {
    if (query.variableQuery?.variableType === VariableType.DistinctValues) {
      fetchDistinctValuesSqlPreview(datasource, {
        tableName: query.tableName,
        columnName: query.variableQuery?.columnName,
      }).then((sqlPreview) => setDistinctValuesSqlPreview(sqlPreview));
    }
  }, [datasource, query.tableName, query.variableQuery?.columnName, query.variableQuery?.variableType]);

  const selectTable = (
    <SelectTable
      selected={query.tableName}
      options={tables}
      onChange={(tableName) => onChange({ ...query, tableName })}
    />
  );

  return (
    <>
      <div className={'gf-form'}>
        <SelectVariableType
          selected={query.variableQuery?.variableType}
          onChange={(variableType) => onChangeVariableQuery({ ...query.variableQuery, variableType })}
        />
      </div>

      {(() => {
        switch (query.variableQuery?.variableType) {
          case VariableType.ColumnList:
            return (
              <ColumnVariableEditor
                selectTable={selectTable}
                variableQuery={query.variableQuery}
                sqlPreview={''}
                columns={[]}
                onChange={onChangeVariableQuery}
              />
            );
          case VariableType.DistinctValues:
            return (
              <DistinctValuesVariableEditor
                selectTable={selectTable}
                variableQuery={query.variableQuery}
                columns={columns}
                onChange={onChangeVariableQuery}
                sqlPreview={distinctValuesSqlPreview}
              />
            );
          case VariableType.PinotQlCode:
            return (
              <SqlVariableEditor
                selectTable={selectTable}
                variableQuery={query.variableQuery}
                onChange={onChangeVariableQuery}
              />
            );
          default:
            return <></>;
        }
      })()}
    </>
  );
}
