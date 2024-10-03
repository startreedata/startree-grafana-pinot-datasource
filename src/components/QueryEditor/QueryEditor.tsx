import React, { lazy, Suspense } from 'react';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';

import { QueryEditorHeader } from './QueryEditorHeader';
import { QueryType } from '../../types/QueryType';

const PinotQlEditor = lazy(() => import('./PinotQlEditor').then((module) => ({ default: module.PinotQlEditor })));
const PromQlEditor = lazy(() => import('./PromQlEditor').then((module) => ({ default: module.PromQlEditor })));

export function QueryEditor(props: PinotQueryEditorProps) {
  return (
    <div>
      <QueryEditorHeader {...props} />
      <Suspense fallback={null}>
        {(() => {
          switch (props.query.queryType) {
            case QueryType.PromQL:
              return <PromQlEditor {...props} />;
            case QueryType.PinotQL:
            default:
              return <PinotQlEditor {...props} />;
          }
        })()}
      </Suspense>
    </div>
  );
}
