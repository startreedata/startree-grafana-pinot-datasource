import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { SqlEditor } from './SqlEditor';
import React from 'react';
import { InputTimeColumnAlias } from './InputTimeColumnAlias';
import { InputMetricColumnAlias } from './InputMetricColumnAlias';
import { InputTimeColumnFormat } from './InputTimeColumnFormat';

export function PinotQlCode(props: PinotQueryEditorProps) {
  const { query, onChange } = props;
  return (
    <div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <InputTimeColumnAlias
          current={query.timeColumnAlias}
          onChange={(val) => onChange({ ...query, timeColumnAlias: val })}
        />
        <InputTimeColumnFormat
          current={query.timeColumnFormat}
          onChange={(val) => onChange({ ...query, timeColumnFormat: val })}
        />
      </div>
      <InputMetricColumnAlias
        current={query.metricColumnAlias}
        onChange={(val) => onChange({ ...props.query, metricColumnAlias: val })}
      />
      <SqlEditor current={query.pinotQlCode} onChange={(val) => onChange({ ...props.query, pinotQlCode: val })} />
    </div>
  );
}
