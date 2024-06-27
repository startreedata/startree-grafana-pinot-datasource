import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { SqlEditor } from './SqlEditor';
import React from 'react';
import { InputTimeColumnAlias } from './InputTimeColumnAlias';
import { InputMetricColumnAlias } from './InputMetricColumnAlias';
import { InputTimeColumnFormat } from './InputTimeColumnFormat';

export function PinotQlCodeEditor(props: PinotQueryEditorProps) {
  return (
    <div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <InputTimeColumnAlias {...props} />
        <InputTimeColumnFormat {...props} />
      </div>
      <InputMetricColumnAlias {...props} />
      <SqlEditor {...props} />
    </div>
  );
}
