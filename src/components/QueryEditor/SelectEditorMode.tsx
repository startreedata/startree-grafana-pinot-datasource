import { ConfirmModal } from './ConfirmModal';
import { EditorMode } from '../../dataquery/EditorMode';
import { RadioButtonGroup } from '@grafana/ui';
import React, { useState } from 'react';
import { PinotDataQuery } from '../../dataquery/PinotDataQuery';
import { DataSource } from '../../datasource';
import { DateTime } from '@grafana/data';
import { DisplayTypeTimeSeries } from './SelectDisplayType';
import { previewSqlBuilder } from '../../resources/previewSql';
import { QueryType } from '../../dataquery/QueryType';
import { builderParamsFrom } from '../../pinotql/builderParams';
import { dataQueryWithCodeParams } from '../../pinotql/codeParams';
import { columnLabelOf } from '../../dataquery/ComplexField';

//language=text
const defaultSql = `SELECT $__timeGroup("timestamp") AS $__timeAlias(), SUM("metric") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("timestamp")
GROUP BY $__timeGroup("timestamp")
ORDER BY $__timeAlias() DESC
LIMIT 100000`;

export function SelectEditorMode(props: {
  query: PinotDataQuery;
  datasource: DataSource;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  onChange: (value: PinotDataQuery) => void;
}) {
  const { query, datasource, intervalSize, timeRange, onChange } = props;
  const [showConfirm, setShowConfirm] = useState(false);

  return (
    <div data-testid="select-editor-mode">
      <ConfirmModal
        isOpen={showConfirm}
        onCopy={() => {
          setShowConfirm(false);
          navigator.clipboard.writeText(query.pinotQlCode || '').then(() =>
            onChange({
              ...query,
              editorMode: EditorMode.Builder,
              pinotQlCode: undefined,
            })
          );
        }}
        onDiscard={() => {
          setShowConfirm(false);
          onChange({
            ...query,
            editorMode: EditorMode.Builder,
            pinotQlCode: undefined,
          });
        }}
        onCancel={() => setShowConfirm(false)}
      />
      {query.queryType === QueryType.PinotQL && (
        <RadioButtonGroup
          options={Object.keys(EditorMode).map((name) => ({ label: name, value: name }))}
          onChange={(value) => {
            if (value === EditorMode.Builder) {
              setShowConfirm(true);
            }
            const builderParams = builderParamsFrom(query);

            if (value === EditorMode.Code) {
              previewSqlBuilder(datasource, {
                intervalSize: intervalSize,
                timeRange: timeRange,
                expandMacros: false,
                aggregationFunction: builderParams.aggregationFunction,
                groupByColumns: builderParams.groupByColumns,
                metricColumn: builderParams.metricColumn,
                tableName: builderParams.tableName,
                timeColumn: builderParams.timeColumn,
                filters: builderParams.filters,
                limit: builderParams.limit,
                granularity: builderParams.granularity,
                orderBy: builderParams.orderBy,
                queryOptions: builderParams.queryOptions,
              }).then((sql) =>
                onChange(
                  dataQueryWithCodeParams(query, {
                    displayType: DisplayTypeTimeSeries,
                    tableName: builderParams.tableName,
                    timeColumnAlias: 'time',
                    metricColumnAlias: columnLabelOf(builderParams.metricColumn.name, builderParams.metricColumn.key),
                    logColumnAlias: '',
                    legend: builderParams.legend,
                    pinotQlCode: sql || defaultSql,
                  })
                )
              );
            }
          }}
          value={query.editorMode}
        />
      )}
    </div>
  );
}
