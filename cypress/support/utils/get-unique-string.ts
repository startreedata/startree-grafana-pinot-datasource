import cryptoRandomString from 'crypto-random-string';

export const getUniqueString = (length = 10): string => {
  return cryptoRandomString({ length: length, type: 'alphanumeric' });
};
