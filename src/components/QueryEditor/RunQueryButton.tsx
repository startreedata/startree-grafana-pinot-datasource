import { Button } from '@grafana/ui';
import React from 'react';

export function RunQueryButton(props: { onRunQuery: () => void }) {
  return (
    <Button data-testid={'run-query-btn'} onClick={() => props.onRunQuery()}>
      Run Query
    </Button>
  );
}
