import React from 'react';
import { InlineFormLabel, PopoverContent } from '@grafana/ui';

export function FormLabel(props: { tooltip: PopoverContent; label: string }) {
  return (
    <InlineFormLabel width={8} className="query-keyword" tooltip={props.tooltip} data-testid="inline-form-label">
      {props.label}
    </InlineFormLabel>
  );
}
