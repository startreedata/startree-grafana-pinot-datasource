import React, { useEffect, useState } from 'react';
import { SelectTable } from './SelectTable';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { FormLabel } from './FormLabel';
import { useTimeSeriesTables } from '../../resources/timeseries';
import { InputMetricLegend } from './InputMetricLegend';
import { useCompletionDataProvider } from '../../promql/completionDataProvider';
import { Button, Icon, Modal } from '@grafana/ui';
import { useIsPromQlSupported } from '../../resources/isPromQlSupported';
import { QueryType } from '../../types/QueryType';
import { DataSource } from '../../datasource';
import { PromQlExpressionEditor } from './PromQlExpressionEditor';

export function PromQlEditor(props: PinotQueryEditorProps) {
  const tables = useTimeSeriesTables(props.datasource);

  const timeRange = {
    to: props.range?.to,
    from: props.range?.from,
  };
  const dataProvider = useCompletionDataProvider(props.datasource, props.query.tableName, timeRange);

  return (
    <>
      <UnsupportedModel
        datasource={props.datasource}
        onClose={() => props.onChange({ ...props.query, queryType: QueryType.PinotQL })}
      />

      <div className={'gf-form'}>
        <SelectTable
          selected={props.query.tableName}
          options={tables}
          onChange={(tableName) => props.onChange({ ...props.query, tableName })}
        />
      </div>
      <div className={'gf-form'}>
        <>
          <FormLabel tooltip={'Query'} label={'Query'} />
          <PromQlExpressionEditor
              datasource={props.datasource}
              value={props.query.promQlCode}
              onExpressionChange={(promQlCode) => props.onChange({ ...props.query, promQlCode })}
              onRunQuery={props.onRunQuery}
          />
        </>
      </div>
      <div className={'gf-form'}>
        <InputMetricLegend
          current={props.query.legend}
          onChange={(legend) => props.onChange({ ...props.query, legend })}
        />
      </div>
    </>
  );
}

function UnsupportedModel(props: { datasource: DataSource; onClose: () => void }) {
  const [isSupported, loading] = useIsPromQlSupported(props.datasource);
  const [showModal, setShowModal] = useState(false);
  const onCloseModal = () => {
    setShowModal(false);
    props.onClose();
  };

  useEffect(() => {
    if (loading) {
      return;
    } else {
      setShowModal(!isSupported);
    }
  }, [isSupported, loading]);

  return (
    <Modal
      title={
        <div className="modal-header-title" data-testid="modal-header-title">
          <Icon name="exclamation-triangle" size="lg" />
          <span className="p-l-1">Warning</span>
        </div>
      }
      onDismiss={() => setShowModal(false)}
      isOpen={showModal}
    >
      <div>
        <p>This version of Pinot does not support Prometheus queries.</p>
      </div>

      <Modal.ButtonRow>
        <Button type="button" variant="primary" onClick={onCloseModal}>
          Return to Pinot QL
        </Button>
        <Button type="button" variant="secondary" onClick={() => setShowModal(false)} fill="outline">
          Ignore
        </Button>
      </Modal.ButtonRow>
    </Modal>
  );
}
