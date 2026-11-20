import { css } from '@emotion/css';
import { config } from '@grafana/runtime';

// Spacing doesn't change between light and dark themes, so the boot theme is enough here.
const spacing = config.theme2.spacing;

export const styles = {
  QueryEditor: {
    inputForm: css`
      margin-right: ${spacing(0.5)};
      width: ${spacing(30)} !important;
    `,
    inlineInputForm: css`
      min-width: ${spacing(15)} !important;
    `,
  },
  VariableQueryEditor: {
    inputForm: css`
      margin-right: ${spacing(0.5)};
      width: ${spacing(26.5)} !important;
    `,
  },
};
