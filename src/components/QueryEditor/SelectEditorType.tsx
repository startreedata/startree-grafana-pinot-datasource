import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { RadioButtonGroup, ToolbarButton } from '@grafana/ui';
import React from 'react';
import { DefaultEditorType, QueryType } from '../../types/QueryType';
import { DefaultEditorMode, EditorMode } from '../../types/EditorMode';
import allLabels from '../../labels';
import { FormLabel } from './FormLabel';

const SupportedQueryTypes = [QueryType.PinotQL];

export function SelectEditorType(props: PinotQueryEditorProps) {
  const { query, onChange, onRunQuery } = props;
  const labels = allLabels.components.QueryEditor.editorType;

  if (query.queryType === undefined || query.editorMode === undefined) {
    onChange({
      ...query,
      queryType: query.queryType || DefaultEditorType,
      editorMode: query.editorMode || DefaultEditorMode,
    });
  }

  return (
    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
      <div className={'gf-form'}>
        <FormLabel tooltip={labels.tooltip} label={labels.label} />

        <RadioButtonGroup
          options={SupportedQueryTypes.map((name) => ({ label: name, value: name }))}
          onChange={(value) => {
            // Manually disable unimplemented options
            switch (value) {
              case QueryType.LogQL:
              case QueryType.PromQL:
                // TODO: Add some unsupported popup
                return;
            }
            onChange({ ...query, queryType: value });
          }}
          value={query.queryType}
        />
      </div>
      <div style={{ display: 'flex' }}>
        <div className={'gf-form'}>
          <RadioButtonGroup
            options={Object.keys(EditorMode).map((name) => ({ label: name, value: name }))}
            onChange={(value) => onChange({ ...query, editorMode: value })}
            value={query.editorMode}
          />
          <ToolbarButton icon={'play'} variant={'primary'} style={{ marginLeft: 4 }} onClick={() => onRunQuery()}>
            Run Query
          </ToolbarButton>
        </div>
      </div>
    </div>
  );
}
