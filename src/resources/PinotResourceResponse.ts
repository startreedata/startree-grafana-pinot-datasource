export interface PinotResourceResponse<T> {
  code: number;
  error: string | null;
  result: T | null;
}
