import { InlineField, Input, PopoverContent } from '@grafana/ui';
import React from 'react';

export function InputUrl(props: {
  label: string;
  tooltip: PopoverContent;
  placeholder: string;
  value: string | undefined;
  onChange: (val: string) => void;
}) {
  const { label, value, tooltip, placeholder, onChange } = props;
  const error = urlError(value);
  const isValid = error === '';

  return (
    <InlineField
      label={label}
      labelWidth={24}
      tooltip={tooltip}
      grow
      required
      invalid={!isValid}
      error={error}
      interactive
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

// URLs are stored in jsonData, which Grafana sends to the browser in plain text, so they must not carry
// credentials. Returns an empty string for a valid (or empty) URL.
export const CREDENTIALS_ERROR =
  'Remove the username and password from the URL. To keep using them, set Token type to Basic and Token to the base64 encoding of username:password.';

export function urlError(url: string | undefined): string {
  if (!url) {
    return '';
  }
  if (!/^(http|https):\/\/(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/.test(url)) {
    return 'Please enter a valid URL';
  }
  try {
    const parsed = new URL(url);
    if (parsed.username !== '' || parsed.password !== '') {
      return CREDENTIALS_ERROR;
    }
  } catch {
    return 'Please enter a valid URL';
  }
  return '';
}
