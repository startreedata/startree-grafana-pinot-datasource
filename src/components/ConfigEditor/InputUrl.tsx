import { InlineField, Input, PopoverContent } from '@grafana/ui';
import React from 'react';

export function InputUrl(props: {
  label: string;
  tooltip: PopoverContent;
  placeholder: string;
  value: string | undefined;
  onChange: (val: string) => void;
}) {
  const isValidUrl = (url: string | undefined) => {
    return url ? /^(http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/.test(url) : true;
  };

  const { label, value, tooltip, placeholder, onChange } = props;
  const isValid = isValidUrl(value);

  return (
    <InlineField
      label={label}
      labelWidth={24}
      tooltip={tooltip}
      grow
      required
      invalid={!isValid}
      error={isValid ? '' : 'Please enter a valid URL'}
      interactive
      data-testid={`${label.toLowerCase().replace(' ', '-')}-inline-field`}
    >
      <Input
        width={40}
        onChange={(event) => onChange(event.currentTarget.value)}
        value={value}
        placeholder={placeholder}
      />
    </InlineField>
  );
}
