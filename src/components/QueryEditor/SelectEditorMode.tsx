import { ConfirmModal } from './ConfirmModal';
import { EditorMode } from '../../types/EditorMode';
import { RadioButtonGroup } from '@grafana/ui';
import React, { useState } from 'react';
import { PinotDataQuery } from '../../types/PinotDataQuery';
import { DataSource } from '../../datasource';
import { DateTime } from '@grafana/data';
import { DisplayTypeTimeSeries } from './SelectDisplayType';
import { previewSqlBuilder } from '../../resources/previewSql';

export function SelectEditorMode(props: {
  query: PinotDataQuery;
  datasource: DataSource;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  onChange: (value: PinotDataQuery) => void;
}) {
  const { query, datasource, onChange } = props;
  const [showConfirm, setShowConfirm] = useState(false);

  return (
    <>
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
        options={Object.keys(EditorMode).map((name) => ({ label: name, value: name }))}
        onChange={(value) => {
          if (value === EditorMode.Builder) {
            setShowConfirm(true);
          }

          if (value === EditorMode.Code) {
            previewSqlBuilder(datasource, {
              aggregationFunction: query.aggregationFunction,
              groupByColumns: query.groupByColumns,
              intervalSize: props.intervalSize || '0',
              metricColumn: query.metricColumn,
              tableName: query.tableName,
              timeColumn: query.timeColumn,
              timeRange: {
                to: props.timeRange.to?.toISOString(),
                from: props.timeRange.from?.toISOString(),
              },
              filters: query.filters,
              limit: query.limit,
              granularity: query.granularity,
              orderBy: query.orderBy,
              queryOptions: query.queryOptions,
              expandMacros: false,
            }).then((sql) =>
              onChange({
                ...query,
                editorMode: EditorMode.Code,
                displayType: DisplayTypeTimeSeries,
                pinotQlCode: sql,
              })
            );
          }
        }}
        value={query.editorMode}
      />
    </>
  );
}
