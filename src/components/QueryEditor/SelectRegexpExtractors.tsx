import React from 'react';
import { RegexpExtractor } from '../../types/PinotDataQuery';
import { Column } from '../../resources/columns';
import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { AccessoryButton } from '@grafana/experimental';
import { EditRegexpExtractor } from './EditRegexpExtractor';

export function SelectRegexpExtractors(props: {
  extractors: RegexpExtractor[];
  columns: Column[];
  isLoadingColumns: boolean;
  onChange: (val: RegexpExtractor[]) => void;
}) {
  const labels = allLabels.components.QueryEditor.regexpExtractors;

  const { extractors, columns, isLoadingColumns, onChange } = props;

  const onChangeField = (val: RegexpExtractor, idx: number) => {
    onChange(extractors?.map((existing, i) => (i === idx ? val : existing)));
  };
  const onDeleteField = (idx: number) => {
    onChange(extractors?.filter((_val, i) => i !== idx));
  };

  return (
    <div className={'gf-form'} data-testid="select-regexp-extractors">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {extractors?.map((field, idx) => (
          <EditRegexpExtractor
            key={idx}
            extractor={field}
            columns={columns}
            isLoadingColumns={isLoadingColumns}
            onChange={(val) => onChangeField(val, idx)}
            onDelete={() => onDeleteField(idx)}
          />
        ))}
        <div>
          <AccessoryButton
            data-testid="add-metadata-field-btn"
            icon="plus"
            variant="secondary"
            fullWidth={false}
            onClick={() => {
              onChange([...(extractors || []), {}]);
            }}
          />
        </div>
      </div>
    </div>
  );
}
