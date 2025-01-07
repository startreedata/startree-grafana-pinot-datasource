import React from 'react';
import { PinotQueryEditorProps } from '../../dataquery/PinotQueryEditorProps';

import { QueryEditorHeader } from './QueryEditorHeader';
import { QueryType } from '../../dataquery/QueryType';
import { PromQlEditor } from './PromQlEditor';
import { PinotQlEditor } from './PinotQlEditor';

export function QueryEditor(props: PinotQueryEditorProps) {
  return (
    <div>
      <QueryEditorHeader {...props} />
      {(() => {
        switch (props.query.queryType) {
          case QueryType.PromQL:
            return <PromQlEditor {...props} />;
          case QueryType.PinotQL:
          default:
            return <PinotQlEditor {...props} />;
        }
      })()}
    </div>
  );
}
