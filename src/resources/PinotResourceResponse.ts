export interface PinotResourceResponse {
  code: number;
  error: string | null;
}

export interface SqlPreviewResponse extends PinotResourceResponse {
  sql: string | null;
}
