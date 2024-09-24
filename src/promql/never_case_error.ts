// Borrowed from grafana

export class NeverCaseError extends Error {
  constructor(value: never) {
    super('should never happen');
  }
}
