import { Column } from '../resources/columns';
import { SelectableValue } from '@grafana/data';
import { ComplexField } from '../dataquery/ComplexField';

export interface FormData {
  options: Array<SelectableValue<string>>;
  usedOption: SelectableValue<string> | null;

  getChange(item: SelectableValue<string>): ComplexField;
}

export interface MultiSelectFormData {
  options: Array<SelectableValue<string>>;
  usedOptions: Array<SelectableValue<string>>;

  getChange(items: Array<SelectableValue<string>>): ComplexField[];
}

export function formDataOf(selected: ComplexField, columns: Column[]): FormData {
  const labelToFieldMap = new Map<string, ComplexField>();
  if (selected.name) {
    labelToFieldMap.set(columnLabelOf(selected.name, selected.key), selected);
  }
  columns
    .filter(({ name }) => name)
    .forEach(({ name, key }) => labelToFieldMap.set(columnLabelOf(name, key), complexFieldOf(name, key)));

  const options = Array.from(labelToFieldMap.values()).map((field) => ({
    label: columnLabelOf(field.name, field.key),
    value: columnLabelOf(field.name, field.key),
  }));

  const usedOption = selected.name
    ? {
        label: columnLabelOf(selected.name, selected.key),
        value: columnLabelOf(selected.name, selected.key),
      }
    : null;

  const getChange = (item: SelectableValue<string>) => {
    return labelToFieldMap.get(item.label || '') || parseColumnName(item.label);
  };

  return { options, usedOption, getChange };
}

export function multiSelectFormDataOf(selected: ComplexField[], columns: Column[]): MultiSelectFormData {
  const labelToFieldMap = new Map<string, ComplexField>();
  selected
    .filter(({ name }) => name)
    .forEach((field) => labelToFieldMap.set(columnLabelOf(field.name, field.key), field));
  columns
    .filter(({ name }) => name)
    .forEach(({ name, key }) => labelToFieldMap.set(columnLabelOf(name, key), complexFieldOf(name, key)));

  const usedOptions = selected.map((field) => ({
    label: columnLabelOf(field.name, field.key),
    value: columnLabelOf(field.name, field.key),
  }));

  const options = Array.from(labelToFieldMap.values()).map((field) => ({
    label: columnLabelOf(field.name, field.key),
    value: columnLabelOf(field.name, field.key),
  }));

  const getComplexField = (label: string | undefined) => {
    return labelToFieldMap.get(label || '') || parseColumnName(label);
  };

  const getChange = (items: Array<SelectableValue<string>>) =>
    items.map(({ label }) => getComplexField(label)).filter(({ name }) => name);

  return { options, usedOptions, getChange };
}

export function parseColumnName(text: string | undefined): ComplexField {
  if (text === undefined) {
    return {};
  } else if (text.includes('[')) {
    const [name, key] = text.trim().split(/[\[\]]/);
    return { name: name.trim(), key: key.trim().replace(/'/g, '') };
  } else {
    return { name: text.trim() };
  }
}

export function columnLabelOf(name: string | undefined, key: string | null | undefined): string {
  if (key) {
    return `${name}['${key}']`;
  } else if (name) {
    return name;
  } else {
    return '';
  }
}

export function complexFieldOf(name: string | undefined, key: string | null | undefined): ComplexField {
  return { name, key: key || undefined };
}
