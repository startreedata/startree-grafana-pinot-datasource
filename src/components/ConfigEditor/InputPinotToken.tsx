import { InlineField, SecretInput, Select } from '@grafana/ui';
import React, { ChangeEvent } from 'react';
import allLabels from '../../labels';

const DefaultTokenType = 'Basic';
const TokenTypeOptions = [
  { label: 'Basic', value: 'Basic' },
  { label: 'Bearer', value: 'Bearer' },
  { label: 'None', value: 'None' },
];

export function InputPinotToken(props: {
  isConfigured: boolean;
  tokenType: string | undefined;
  tokenValue: string | undefined;
  onChangeType: (val: string | undefined) => void;
  onChangeToken: (val: string | undefined) => void;
  onResetToken: () => void;
}) {
  const { isConfigured, tokenType, tokenValue, onChangeToken, onChangeType, onResetToken } = props;
  const labels = allLabels.components.ConfigEditor.token;

  if (tokenType === undefined && tokenType !== DefaultTokenType) {
    onChangeType(DefaultTokenType);
  }

  return (
    <div className={'gf-form'}>
      <InlineField label={labels.typeLabel} labelWidth={8} required>
        <Select
          options={TokenTypeOptions}
          isSearchable={false}
          value={tokenType || DefaultTokenType}
          width={12}
          onChange={(change) => onChangeType(change.value)}
        />
      </InlineField>
      {tokenType != 'None' && (
        <InlineField label={labels.valueLabel} labelWidth={8} required>
          <SecretInput
            isConfigured={isConfigured}
            value={tokenValue}
            placeholder={labels.valuePlaceholder}
            width={40}
            onReset={onResetToken}
            onChange={(event: ChangeEvent<HTMLInputElement>) => onChangeToken(event.target.value)}
          />
        </InlineField>
      )}
    </div>
  );
}
