export enum QueryType {
  PinotQL = 'PinotQL',
  PromQL = 'PromQL',
  LogQL = 'LogQL',
  PinotVariableQuery = 'PinotVariableQuery',
}

export const DefaultEditorType = QueryType.PinotQL;
