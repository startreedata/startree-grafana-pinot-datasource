import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { SelectDatabase } from './SelectDatabase';
import { SelectTable } from './SelectTable';
import { EditorMode } from '../types/EditorMode';
import React from 'react';
import { PinotQlBuilderEditor } from './PinotQlBuilderEditor';
import { PinotQlCodeEditor } from './PinotQlCodeEditor';

export function PinotQlEditor(props: PinotQueryEditorProps) {
  return (
    <div>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <div className={'gf-form'}>
          <SelectDatabase {...props} />
          <SelectTable {...props} />
        </div>
      </div>
      {props.query.editorMode == EditorMode.Code ? (
        <PinotQlCodeEditor {...props} />
      ) : (
        <PinotQlBuilderEditor {...props} />
      )}
    </div>
  );
}
