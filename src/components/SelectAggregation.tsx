import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { InlineFormLabel, Select } from '@grafana/ui';
import { styles } from '../styles';
import React from 'react';
import { canRunQuery } from '../types/PinotDataQuery';

export function SelectAggregation(props: PinotQueryEditorProps) {
  const { query, onChange, onRunQuery } = props;

  // TODO: Where do these belong more permanently?
  const aggFunctions = ['SUM', 'COUNT', 'AVG', 'MAX'];
  return (
    <div className={'gf-form'}>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select aggregation function'}>
        Aggregation
      </InlineFormLabel>
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={aggFunctions.map((name) => ({ label: name, value: name }))}
        value={query.aggregationFunction}
        onChange={(value) => {
          const newQuery = { ...query, aggregationFunction: value.value }
          onChange(newQuery);
          if (canRunQuery(newQuery)) {
            onRunQuery();
          }
        }}
      />
    </div>
  );
}
