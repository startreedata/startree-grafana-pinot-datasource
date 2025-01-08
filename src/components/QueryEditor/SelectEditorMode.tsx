import { ConfirmModal } from './ConfirmModal';
import { EditorMode } from '../../dataquery/EditorMode';
import { RadioButtonGroup } from '@grafana/ui';
import React, { useState } from 'react';
import { PinotDataQuery } from '../../dataquery/PinotDataQuery';
import { DataSource } from '../../datasource';
import { DateTime } from '@grafana/data';
import { previewSqlBuilder } from '../../resources/previewSql';
import { QueryType } from '../../dataquery/QueryType';
import { columnLabelOf } from '../../dataquery/ComplexField';
import { DisplayType } from '../../dataquery/DisplayType';
import { TimeSeriesBuilder } from '../../pinotql/TimeSeriesBuilder';
import { CodeQuery } from '../../pinotql/CodeQuery';

export function SelectEditorMode(props: {
  query: PinotDataQuery;
  datasource: DataSource;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  onChange: (value: PinotDataQuery) => void;
}) {
  const { query, datasource, intervalSize, timeRange, onChange } = props;
  const [showConfirm, setShowConfirm] = useState(false);

  if (query.queryType !== QueryType.PinotQL) {
    return <></>;
  }

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
      <RadioButtonGroup
        data-testid="radio"
        options={Object.keys(EditorMode).map((name) => ({ label: name, value: name }))}
        onChange={(value) => {
          if (value === EditorMode.Builder) {
            setShowConfirm(true);
          }
          const builderParams = TimeSeriesBuilder.paramsFrom(query);

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
                CodeQuery.dataQueryOf(query, {
                  displayType: DisplayType.TIMESERIES,
                  tableName: builderParams.tableName,
                  timeColumnAlias: 'time',
                  metricColumnAlias: columnLabelOf(builderParams.metricColumn.name, builderParams.metricColumn.key),
                  logColumnAlias: '',
                  legend: builderParams.legend,
                  pinotQlCode: sql,
                })
              )
            );
          }
        }}
        value={query.editorMode}
      />
    </div>
  );
}
