import React from 'react';
import { InlineFormLabel, PopoverContent } from '@grafana/ui';

export function FormLabel(props: { tooltip: PopoverContent; label: string; required?: boolean }) {
  return (
    <InlineFormLabel width={9} className="query-keyword" tooltip={props.tooltip}>
      {props.label}
      {props.required ? ' *' : ''}
    </InlineFormLabel>
  );
}
