export enum CompletionType {
  FUNCTION,
  DURATION,
  METRIC_NAME,
  LABEL_NAME,
  LABEL_VALUE,
}

export interface Completion {
  type: CompletionType;
  label: string;
  insertText: string;
  detail?: string;
  documentation?: string;
  triggerOnInsert?: boolean;
}

