import { createHash, Hash, BinaryToTextEncoding } from 'crypto';

export function hashSync(data: string, alg?: string, encoding?: BinaryToTextEncoding): string {
  let hash: Hash, hashStr: string;
  if(alg === undefined) {
    alg = 'sha256';
  }
  if(encoding === undefined) {
    encoding = 'base64';
  }
  hash = createHash(alg);
  hash.update(data);
  hashStr = hash.digest(encoding);
  return hashStr;
}
