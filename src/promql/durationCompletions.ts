import { Completion, CompletionType } from './completion';

export const DURATION_COMPLETIONS: Completion[] = [
  '$__interval',
  '$__range',
  '$__rate_interval',
  '1m',
  '5m',
  '10m',
  '30m',
  '1h',
  '1d',
].map((text) => ({
  type: CompletionType.DURATION,
  label: text,
  insertText: text,
}));
