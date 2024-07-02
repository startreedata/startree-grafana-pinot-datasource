import React from 'react';
import { InlineFormLabel } from '@grafana/ui';

export function FormLabel(props: { tooltip: string; label: string }) {
  return (
    <InlineFormLabel width={8} className="query-keyword" tooltip={props.tooltip}>
      {props.label}
    </InlineFormLabel>
  );
}
