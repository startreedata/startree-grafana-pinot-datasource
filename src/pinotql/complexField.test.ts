import { columnLabelOf, complexFieldOf, formDataOf, multiSelectFormDataOf, parseColumnName } from './complexField';
import { ComplexField } from '../dataquery/ComplexField';
import { Column } from '../resources/columns';
import { PinotDataType } from '../dataquery/PinotDataType';
import { SelectableValue } from '@grafana/data';

describe('formDataOf', () => {
  const columns: Column[] = [
    { name: 'name1', key: 'key1', dataType: PinotDataType.STRING, isDerived: false, isTime: false, isMetric: false },
    { name: 'name2', key: 'key2', dataType: PinotDataType.STRING, isDerived: false, isTime: false, isMetric: false },
  ];

  test('should return columns and selected column as options', () => {
    const selected: ComplexField = { name: 'custom', key: 'customKey' };
    expect(formDataOf(selected, columns).options).toEqual<Array<SelectableValue<string>>>([
      { label: "custom['customKey']", value: "custom['customKey']" },
      { label: "name1['key1']", value: "name1['key1']" },
      { label: "name2['key2']", value: "name2['key2']" },
    ]);
  });

  test('should return selected column as used option', () => {
    const selected: ComplexField = { name: 'custom', key: 'customKey' };
    expect(formDataOf(selected, columns).usedOption).toEqual<SelectableValue<string>>({
      label: "custom['customKey']",
      value: "custom['customKey']",
    });
  });

  test('should return empty used option if no selected column', () => {
    const selected: ComplexField = { name: undefined, key: undefined };
    expect(formDataOf(selected, columns).usedOption).toEqual<SelectableValue<string> | null>(null);
  });

  test('getChange should return selected column if it exists', () => {
    const selected: ComplexField = { name: undefined, key: undefined };
    expect(
      formDataOf(selected, columns).getChange({
        label: "name1['key1']",
        value: "name1['key1']",
      })
    ).toEqual<ComplexField>({
      name: 'name1',
      key: 'key1',
    });
  });

  test('getChange should parse the label if the column does not exist', () => {
    const selected: ComplexField = { name: undefined, key: undefined };
    expect(
      formDataOf(selected, columns).getChange({
        label: "custom['customKey']",
        value: "custom['customKey']",
      })
    ).toEqual<ComplexField>({
      name: 'custom',
      key: 'customKey',
    });
  });
});

describe('multiSelectFormDataOf', () => {
  const columns: Column[] = [
    { name: 'name1', key: 'key1', dataType: PinotDataType.STRING, isDerived: false, isTime: false, isMetric: false },
    { name: 'name2', key: 'key2', dataType: PinotDataType.STRING, isDerived: false, isTime: false, isMetric: false },
  ];

  test('should return columns and selected columns as options', () => {
    const selected: ComplexField[] = [
      { name: 'custom1', key: 'customKey1' },
      { name: 'name1', key: 'key1' },
    ];
    expect(multiSelectFormDataOf(selected, columns).options).toEqual<Array<SelectableValue<string>>>([
      { label: "custom1['customKey1']", value: "custom1['customKey1']" },
      { label: "name1['key1']", value: "name1['key1']" },
      { label: "name2['key2']", value: "name2['key2']" },
    ]);
  });

  test('should return selected columns as used options', () => {
    const selected: ComplexField[] = [
      { name: 'custom1', key: 'customKey1' },
      { name: 'name1', key: 'key1' },
    ];
    expect(multiSelectFormDataOf(selected, columns).usedOptions).toEqual<Array<SelectableValue<string>>>([
      { label: "custom1['customKey1']", value: "custom1['customKey1']" },
      { label: "name1['key1']", value: "name1['key1']" },
    ]);
  });

  test('should return empty used options if no selected columns', () => {
    const selected: ComplexField[] = [];
    expect(multiSelectFormDataOf(selected, columns).usedOptions).toEqual<Array<SelectableValue<string>>>([]);
  });

  describe('getChange', () => {
    it('should return empty array when no items are selected', () => {
      const selected: ComplexField[] = [];
      expect(multiSelectFormDataOf(selected, columns).getChange([])).toEqual([]);
    });

    it('should return complex fields when columns are selected', () => {
      const selected: ComplexField[] = [];
      expect(
        multiSelectFormDataOf(selected, columns).getChange([
          { label: "name1['key1']", value: "name1['key1']" },
          { label: "customCol['customKey']", value: "customCol['customKey']" },
        ])
      ).toEqual<ComplexField[]>([
        { name: 'name1', key: 'key1' },
        { name: 'customCol', key: 'customKey' },
      ]);
    });
  });
});

describe('columnLabelOf', () => {
  test('should return name if key is not provided', () => {
    expect(columnLabelOf('name', undefined)).toBe('name');
  });
  test('should return name[key] if key is provided', () => {
    expect(columnLabelOf('name', 'key')).toBe("name['key']");
  });
});

describe('parseColumnName', () => {
  test('should return empty object if value is undefined', () => {
    expect(parseColumnName(undefined)).toEqual({});
  });
  test('should return name if key is not provided', () => {
    expect(parseColumnName('name')).toEqual({ name: 'name' });
  });
  test('should return name and key if key is provided', () => {
    expect(parseColumnName("name['key']")).toEqual({ name: 'name', key: 'key' });
  });
});

describe('complexFieldOf', () => {
  test('should return complex field with name and key', () => {
    expect(complexFieldOf('name', 'key')).toEqual<ComplexField>({
      name: 'name',
      key: 'key',
    });
  });
  test('should return complex field with name and empty key', () => {
    expect(complexFieldOf('name', null)).toEqual<ComplexField>({
      name: 'name',
      key: undefined,
    });
  });
});
