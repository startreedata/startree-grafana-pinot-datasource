import React from 'react';
import { InlineSwitch } from '@grafana/ui';
import { FormLabel } from './FormLabel';

export function ToggleSqlPreview(props: { value: boolean; onChange: () => void }) {
  const { value, onChange } = props;

  return (
    <div className="gf-form">
      <FormLabel tooltip={<p>{'Toggle SQL Preview'}</p>} label={'Show SQL Preview'} />
      <InlineSwitch value={value} onClick={onChange} />
    </div>
  );
}
