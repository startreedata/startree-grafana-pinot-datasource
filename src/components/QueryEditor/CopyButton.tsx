import allLabels from '../../labels';
import React, { useState } from 'react';
import { IconButton } from '@grafana/ui';

export function CopyButton({ text }: { text: string }) {
  const labels = allLabels.components.QueryEditor.sqlPreview;
  const [isCopied, setIsCopied] = useState(false);

  return (
    <div
      style={{ paddingTop: 0, display: 'flex', flexDirection: 'row-reverse' }}
      onMouseLeave={() => setIsCopied(false)}
    >
      <IconButton
        data-testid="copy-query-btn"
        name={'copy'}
        tooltip={isCopied ? labels.copiedTooltip : labels.copyTooltip}
        onClick={() => {
          navigator.clipboard.writeText(text).then(() => setIsCopied(true));
        }}
      />
    </div>
  );
}
