import { formDataOf } from './orderBy';
import { SelectableValue } from '@grafana/data';

describe('formDataOf', () => {
  const selected = [
    { columnName: 'column1', columnKey: 'key1', direction: 'ASC' },
    { columnName: 'column2', columnKey: 'key2', direction: 'DESC' },
  ];
  const columns = [
    { name: 'column1', key: 'key1' },
    { name: 'column2', key: 'key2' },
    { name: 'column3', key: 'key3' },
  ];

  it('should return form data with expected options', () => {
    const result = formDataOf(selected, columns);
    expect(result.options).toEqual<Array<SelectableValue<string>>>([
      { label: "column1['key1'] asc", value: "column1['key1'] asc" },
      { label: "column2['key2'] desc", value: "column2['key2'] desc" },
      { label: "column3['key3'] asc", value: "column3['key3'] asc" },
      { label: "column3['key3'] desc", value: "column3['key3'] desc" },
    ]);
  });

  it('should return form data with expected used options', () => {
    const result = formDataOf(selected, columns);
    expect(result.usedOptions).toEqual<Array<SelectableValue<string>>>([
      { label: "column1['key1'] asc", value: "column1['key1'] asc" },
      { label: "column2['key2'] desc", value: "column2['key2'] desc" },
    ]);
  });

  describe('getChange', () => {
    it('should return empty array when no items are selected', () => {
      expect(formDataOf(selected, columns).getChange([])).toEqual([]);
    });

    it('should return order by clauses when columns are selected', () => {
      expect(
        formDataOf(selected, columns).getChange([
          { label: "column1['key1'] asc", value: "column1['key1'] asc" },
          { label: "column3['key3'] desc", value: "column3['key3'] desc" },
          { label: "customCol['customKey'] asc", value: "customCol['customKey'] asc" },
        ])
      ).toEqual([
        { columnName: 'column1', columnKey: 'key1', direction: 'ASC' },
        { columnName: 'column3', columnKey: 'key3', direction: 'DESC' },
        { columnName: 'customCol', columnKey: 'customKey', direction: 'ASC' },
      ]);
    });
  });
});
