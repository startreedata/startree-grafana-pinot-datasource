import React from 'react';
import { PinotQueryEditorProps } from '../../dataquery/PinotQueryEditorProps';
import { DisplayType } from '../../dataquery/DisplayType';
import { QueryEditor } from '../QueryEditor/QueryEditor';
import { EditorMode } from '../../dataquery/EditorMode';

export function AnnotationsQueryEditor(props: PinotQueryEditorProps) {
  props.query.displayType = DisplayType.ANNOTATIONS;
  props.query.editorMode = EditorMode.Code;
  return <QueryEditor {...props} />;
}
