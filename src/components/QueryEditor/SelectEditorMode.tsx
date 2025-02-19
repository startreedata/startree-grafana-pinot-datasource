import { ConfirmModal } from './ConfirmModal';
import { EditorMode } from '../../dataquery/EditorMode';
import { RadioButtonGroup } from '@grafana/ui';
import React, { useState } from 'react';
import { PinotDataQuery } from '../../dataquery/PinotDataQuery';
import { DataSource } from '../../datasource';
import { DateTime } from '@grafana/data';
import { previewLogsSql, previewSqlBuilder } from '../../resources/previewSql';
import { QueryType } from '../../dataquery/QueryType';
import { DisplayType } from '../../dataquery/DisplayType';
import { CodeQuery, LogsBuilder, TimeSeriesBuilder } from '../../pinotql';

export function SelectEditorMode(props: {
  query: PinotDataQuery;
  datasource: DataSource;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  onChange: (value: PinotDataQuery) => void;
}) {
  const { query, datasource, intervalSize, timeRange, onChange } = props;
  const [showConfirm, setShowConfirm] = useState(false);

  if (query.queryType !== QueryType.PinotQL || query.displayType === DisplayType.ANNOTATIONS) {
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
        onChange={(newValue) => {
          if (newValue === EditorMode.Builder) {
            setShowConfirm(true);
          } else if (query.displayType === DisplayType.LOGS) {
            const builderParams = LogsBuilder.paramsFrom(query);
            previewLogsSql(datasource, {
              ...builderParams,
              timeRange: timeRange,
              expandMacros: false,
            }).then((sql) =>
              onChange(CodeQuery.dataQueryOf(query, CodeQuery.paramsFromLogsBuilder(builderParams, sql)))
            );
          } else if (query.displayType === DisplayType.TIMESERIES) {
            const builderParams = TimeSeriesBuilder.paramsFrom(query);
            previewSqlBuilder(datasource, {
              ...builderParams,
              intervalSize: intervalSize,
              timeRange: timeRange,
              expandMacros: false,
            }).then((sql) =>
              onChange(CodeQuery.dataQueryOf(query, CodeQuery.paramsFromTimeSeriesBuilder(builderParams, sql)))
            );
          }
        }}
        value={query.editorMode}
      />
    </div>
  );
}
