export interface ComplexField {
  name: string;
  key?: string | null;
}

export function getColumnLabel(name: string, key?: string | null): string {
  return key ? `${name}['${key}']` : name;
}
