import { Button, Icon, Modal } from '@grafana/ui';
import React, { useEffect, useRef } from 'react';

type ConfirmModalProps = {
  isOpen: boolean;
  onCancel?: () => void;
  onDiscard?: () => void;
  onCopy?: () => void;
};

export function ConfirmModal({ isOpen, onCancel, onDiscard, onCopy }: ConfirmModalProps) {
  const buttonRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (isOpen) {
      buttonRef.current?.focus();
    }
  }, [isOpen]);

  return (
    <Modal
      title={
        <div className="modal-header-title" data-testid="modal-header-title">
          <Icon name="exclamation-triangle" size="lg" />
          <span className="p-l-1">Warning</span>
        </div>
      }
      onDismiss={onCancel}
      isOpen={isOpen}
    >
      <div data-testid="modal-body">
        <p>
          Builder mode does not display changes made in code. The query builder will display the last changes you made
          in builder mode.
        </p>
        <p>Do you want to copy your code to the clipboard?</p>
      </div>

      <Modal.ButtonRow>
        <Button type="button" variant="secondary" onClick={onCancel} fill="outline" data-testid="cancel-btn">
          Cancel
        </Button>
        <Button
          variant="destructive"
          type="button"
          onClick={onDiscard}
          ref={buttonRef}
          data-testid="discard-code-and-switch-btn"
        >
          Discard code and switch
        </Button>
        <Button variant="primary" onClick={onCopy} data-testid="copy-code-and-switch-btn">
          Copy code and switch
        </Button>
      </Modal.ButtonRow>
    </Modal>
  );
}
