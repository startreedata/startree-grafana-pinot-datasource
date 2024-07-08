import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { RadioButtonGroup } from '@grafana/ui';
import React from 'react';
import { QueryType } from '../types/QueryType';
import { EditorMode } from '../types/EditorMode';
import allLabels from '../labels';
import { FormLabel } from './FormLabel';

export function SelectEditorType(props: PinotQueryEditorProps) {
  const { query, onChange } = props;
  const labels = allLabels.components.QueryEditor.editorType;

  return (
    <div className={'gf-form'} style={{ display: 'flex', justifyContent: 'space-between' }}>
      <div className={'gf-form'}>
        <FormLabel tooltip={labels.tooltip} label={labels.label} />
        <RadioButtonGroup
          options={Object.keys(QueryType).map((name) => ({ label: name, value: name }))}
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
      <div className={'gf-form'}>
        <RadioButtonGroup
          options={Object.keys(EditorMode).map((name) => ({ label: name, value: name }))}
          onChange={(value) => onChange({ ...query, editorMode: value })}
          value={query.editorMode}
        />
      </div>
    </div>
  );
}
