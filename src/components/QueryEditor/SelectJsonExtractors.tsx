import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { Column } from '../../resources/columns';
import { JsonExtractor } from '../../dataquery/JsonExtractor';
import { EditJsonExtractor } from './EditJsonExtractor';
import { Button } from '@grafana/ui';

export function SelectJsonExtractors(props: {
  extractors: JsonExtractor[];
  columns: Column[];
  isLoadingColumns: boolean;
  onChange: (val: JsonExtractor[]) => void;
}) {
  const labels = allLabels.components.QueryEditor.jsonExtractors;

  const { extractors, columns, isLoadingColumns, onChange } = props;

  const onChangeField = (val: JsonExtractor, idx: number) => {
    onChange(extractors?.map((existing, i) => (i === idx ? val : existing)));
  };
  const onDeleteField = (idx: number) => {
    onChange(extractors?.filter((_val, i) => i !== idx));
  };

  return (
    <div className={'gf-form'} data-testid="select-json-extractors">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {extractors?.map((field, idx) => (
          <EditJsonExtractor
            key={idx}
            extractor={field}
            columns={columns}
            isLoadingColumns={isLoadingColumns}
            onChange={(val) => onChangeField(val, idx)}
            onDelete={() => onDeleteField(idx)}
          />
        ))}
        <div>
          <Button
            data-testid="add-json-extractor-btn"
            icon="plus"
            variant="secondary"
            fullWidth={false}
            aria-label="Add json extractor"
            onClick={() => {
              onChange([...(extractors || []), {}]);
            }}
          />
        </div>
      </div>
    </div>
  );
}
