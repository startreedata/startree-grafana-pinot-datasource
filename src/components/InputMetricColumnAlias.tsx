import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { InlineFormLabel, Input } from '@grafana/ui';
import React, { ChangeEvent } from 'react';
import { styles } from '../styles';

export function InputMetricColumnAlias(props: PinotQueryEditorProps) {
  const { query, onChange } = props;

  return (
    <div className={'gf-form'}>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Metric column alias.'}>
        Metric Alias
      </InlineFormLabel>
      <Input
        className={`width-15 ${styles.Common.inlineSelect}`}
        onChange={(event: ChangeEvent<HTMLInputElement>) =>
          onChange({
            ...query,
            metricColumnAlias: event.target.value,
          })
        }
        value={query.metricColumnAlias || 'metric'}
        width={24}
      />
    </div>
  );
}
