export interface ComplexField {
  name?: string;
  key?: string;
}

export function columnLabelOf(name?: string, key?: string | null): string {
  if (key) {
    return `${name}['${key}']`;
  } else if (name) {
    return name;
  } else {
    return '';
  }
}
