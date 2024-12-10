export const PinotDataType = Object.freeze({
  INT: 'INT',
  LONG: 'LONG',
  FLOAT: 'FLOAT',
  DOUBLE: 'DOUBLE',
  STRING: 'STRING',
  MAP: 'MAP',
  BYTES: 'BYTES',
  BIG_DECIMAL: 'BIG_DECIMAL',
});

export const PinotDataTypes = Object.values(PinotDataType);
export const NumericPinotDataTypes = [
  PinotDataType.INT,
  PinotDataType.LONG,
  PinotDataType.FLOAT,
  PinotDataType.DOUBLE,
  PinotDataType.BIG_DECIMAL,
];
