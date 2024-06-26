import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { InlineFormLabel, RadioButtonGroup } from '@grafana/ui';
import React from 'react';
import {QueryType} from "../types/QueryType";
import {EditorMode} from "../types/EditorMode";

export function SelectEditorType(props: PinotQueryEditorProps) {
  const { query, onChange } = props;

  return (
    <div className={'gf-form'} style={{ display: 'flex', justifyContent: 'space-between' }}>
      <div className={'gf-form'}>
        <InlineFormLabel width={8} className="query-keyword" tooltip={'Select query type'}>
          Query Type
        </InlineFormLabel>
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
