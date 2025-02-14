import React, { useEffect, useState } from 'react';
import { SelectTable } from './SelectTable';
import { PinotQueryEditorProps } from '../../dataquery/PinotQueryEditorProps';
import { FormLabel } from './FormLabel';
import { useTimeSeriesTables } from '../../resources/timeseries';
import { InputMetricLegend } from './InputMetricLegend';
import { Button, Icon, Modal } from '@grafana/ui';
import { useIsPromQlSupported } from '../../resources/isPromQlSupported';
import { QueryType } from '../../dataquery/QueryType';
import { DataSource } from '../../datasource';
import { PromQlExpressionEditor } from './PromQlExpressionEditor';
import { InputSeriesLimit } from './InputLimit';
import { dataQueryOf, Params, paramsFrom } from '../../promql/params';

export function PromQlEditor(props: PinotQueryEditorProps) {
  const { result: tables, loading: isTablesLoading } = useTimeSeriesTables(props.datasource);
  const params = paramsFrom(props.query);
  const onChange = (newParams: Params) => props.onChange(dataQueryOf(props.query, newParams));
  const timeRange = {
    to: props.range?.to,
    from: props.range?.from,
  };

  return (
    <>
      <UnsupportedModel
        datasource={props.datasource}
        onClose={() => props.onChange({ ...props.query, queryType: QueryType.PinotQL })}
      />

      <SelectTable
        selected={params.tableName || ''}
        options={tables || []}
        isLoading={isTablesLoading}
        onChange={(tableName) => onChange({ ...params, tableName })}
      />
      <div className={'gf-form'}>
        <>
          <FormLabel tooltip={'Query'} label={'Query'} />
          <PromQlExpressionEditor
            datasource={props.datasource}
            tableName={params.tableName}
            timeRange={timeRange}
            value={params.promQlCode}
            onChange={(promQlCode) => onChange({ ...params, promQlCode })}
            onRunQuery={props.onRunQuery}
          />
        </>
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <InputMetricLegend current={params.legend} onChange={(legend) => onChange({ ...params, legend })} />
        <InputSeriesLimit
          current={params.seriesLimit}
          onChange={(seriesLimit) => onChange({ ...params, seriesLimit })}
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
