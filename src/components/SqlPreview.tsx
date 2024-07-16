import React, { useState } from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';
import { IconButton } from '@grafana/ui';

export function SqlPreview(props: { sql: string | undefined }) {
  const { sql } = props;
  const labels = allLabels.components.QueryEditor.sqlPreview;

  const [isCopied, setIsCopied] = useState(false);

  return (
    <div className="gf-form">
      <FormLabel tooltip={<p>labels.tooltip</p>} label={labels.label} />
      <pre>
        {sql || ''}
        {sql && (
          <div style={{ paddingTop: 0, display: 'flex', flexDirection: 'row-reverse' }}
            onMouseLeave={() => setIsCopied(false)}
          >
            <IconButton
              name={'copy'}
              tooltip={isCopied ? labels.copiedTooltip : labels.copyTooltip}
              onClick={() => {
                setIsCopied(true);
                navigator.clipboard.writeText(sql);
              }}
            />
          </div>
        )}
      </pre>
    </div>
  );
}
