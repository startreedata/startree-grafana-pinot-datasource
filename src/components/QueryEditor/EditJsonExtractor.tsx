import React, { ChangeEvent } from 'react';
import { InputGroup } from '@grafana/experimental';
import { Button, Input, Select } from '@grafana/ui';
import { JsonExtractor, ResultTypes } from '../../dataquery/JsonExtractor';
import { Column } from '../../resources/columns';
import { formDataOf } from '../../pinotql/complexField';

export function EditJsonExtractor(props: {
  extractor: JsonExtractor;
  isLoadingColumns: boolean;
  columns: Column[];
  onChange: (v: JsonExtractor) => void;
  onDelete: () => void;
}) {
  const { extractor, columns, isLoadingColumns, onChange, onDelete } = props;
  const columnFormData = formDataOf(extractor.source || {}, columns);
  const resultTypeOptions = ResultTypes.map((t) => ({ label: t, value: t }));
  return (
    <InputGroup data-testid={'edit-json-extractor'}>
      <div data-testid="json-extractor-select-column">
        <Select
          placeholder="Column"
          width="auto"
          value={columnFormData.usedOption}
          allowCustomValue
          options={columnFormData.options}
          isLoading={isLoadingColumns}
          onChange={(item) => {
            const col = columnFormData.getChange(item);
            onChange({
              ...extractor,
              source: { name: col?.name, key: col?.key || undefined },
            });
          }}
        />
      </div>
      <div data-testid="json-extractor-input-path">
        <Input
          onChange={(event: ChangeEvent<HTMLInputElement>) =>
            onChange({
              ...extractor,
              path: event.target.value,
            })
          }
          placeholder={'$.key'}
          value={extractor.path}
        />
      </div>
      <div data-testid="json-extractor-select-result-type">
        <Select
          placeholder="Result type"
          width="auto"
          value={extractor.resultType}
          options={resultTypeOptions}
          isLoading={isLoadingColumns}
          onChange={(change) => onChange({ ...extractor, resultType: change.value })}
        />
      </div>
      <div data-testid="json-extractor-input-alias">
        <Input
          width={15}
          onChange={(event: ChangeEvent<HTMLInputElement>) =>
            onChange({
              ...extractor,
              alias: event.target.value,
            })
          }
          placeholder={'Alias'}
          value={extractor.alias}
        />
      </div>
      <Button data-testid="delete-metadata-field-btn" icon="times" variant="secondary" aria-label="Delete metadata field" onClick={onDelete} />
    </InputGroup>
  );
}
