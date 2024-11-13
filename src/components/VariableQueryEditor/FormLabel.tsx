import { InlineFormLabel } from '@grafana/ui';
import React from 'react';

export function FormLabel(props: { label: string; tooltip: string }) {
  return (
    <InlineFormLabel width={10} tooltip={props.tooltip} data-testid="inline-form-label">
      {props.label}
    </InlineFormLabel>
  );
}
