import { RegexpExtractor } from '../../dataquery/RegexpExtractor';
import { Column } from '../../resources/columns';
import { AccessoryButton, InputGroup } from '@grafana/experimental';
import { Input, Select } from '@grafana/ui';
import React, { ChangeEvent, useState } from 'react';
import { formDataOf } from '../../pinotql/complexField';

export function EditRegexpExtractor(props: {
  extractor: RegexpExtractor;
  isLoadingColumns: boolean;
  columns: Column[];
  onChange: (v: RegexpExtractor) => void;
  onDelete: () => void;
}) {
  const { extractor, columns, isLoadingColumns, onChange, onDelete } = props;
  const columnFormData = formDataOf(extractor.source || {}, columns);
  const [pattern, setPattern] = useState<string>(extractor.pattern || '');
  const [isPatternValid, setIsPatternValid] = useState<boolean>(true);

  const groupOptions = () => {
    try {
      const pattern = new RegExp(extractor.pattern + '|');
      const nGroups = pattern.exec('')?.length || 0;
      return new Array(nGroups).fill(null).map((_, i) => ({ label: i.toString(), value: i }));
    } catch (e) {
      return [{ label: '0', value: 0 }];
    }
  };

  return (
    <InputGroup data-testid={'edit-regexp-extractor'}>
      <div data-testid="regexp-extractor-select-column">
        <Select
          placeholder="Column"
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
      <div data-testid="regexp-extractor-input-pattern">
        <Input
          width={30}
          onChange={(event: ChangeEvent<HTMLInputElement>) => {
            const newPattern = event.target.value;
            setPattern(newPattern);
            try {
              new RegExp(newPattern);
              setIsPatternValid(true);
              onChange({ ...extractor, pattern: newPattern });
            } catch (e) {
              setIsPatternValid(false);
              onChange({ ...extractor, pattern: '' });
            }
          }}
          placeholder={'.*'}
          invalid={!isPatternValid}
          value={pattern}
        />
      </div>
      <div data-testid="regexp-extractor-select-group">
        <Select
          placeholder="Group"
          width={12}
          value={extractor.group}
          allowCustomValue
          options={groupOptions()}
          onChange={(change) => onChange({ ...extractor, group: change.value })}
        />
      </div>
      <div data-testid="regexp-extractor-input-alias">
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
      <AccessoryButton data-testid="delete-metadata-field-btn" icon="times" variant="secondary" onClick={onDelete} />
    </InputGroup>
  );
}
