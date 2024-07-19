import React from 'react';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { EditorMode } from '../../types/EditorMode';
import { PinotQlBuilder } from './PinotQlBuilder';
import { PinotQlCode } from './PinotQlCode';

export function PinotQlEditor(props: PinotQueryEditorProps) {
  return (
    <div>{props.query.editorMode === EditorMode.Code ? <PinotQlCode {...props} /> : <PinotQlBuilder {...props} />}</div>
  );
}
