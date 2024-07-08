export const PinotDataType = Object.freeze({
  INT: 'INT',
  LONG: 'LONG',
  FLOAT: 'FLOAT',
  DOUBLE: 'DOUBLE',
  STRING: 'STRING',
});

export const PinotDataTypes = Object.values(PinotDataType);
export const NumericPinotDataTypes = [PinotDataType.INT, PinotDataType.LONG, PinotDataType.FLOAT, PinotDataType.DOUBLE];
