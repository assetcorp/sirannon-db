#include "prototype-crypto.h"

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <string.h>

int prototypeRandomBytes(unsigned char *out, int length) {
  return RAND_bytes(out, length) == 1 ? 0 : 1;
}

int prototypeSeal(const unsigned char *key, const unsigned char *nonce, const unsigned char *aad, int aadLength,
                  const unsigned char *plaintext, int length, unsigned char *ciphertext, unsigned char *tag) {
  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  int written = 0;
  int ok = ctx != 0;
  if (ok) ok = EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), 0, key, nonce) == 1;
  if (ok && aadLength > 0) ok = EVP_EncryptUpdate(ctx, 0, &written, aad, aadLength) == 1;
  if (ok) ok = EVP_EncryptUpdate(ctx, ciphertext, &written, plaintext, length) == 1;
  if (ok) ok = EVP_EncryptFinal_ex(ctx, ciphertext + written, &written) == 1;
  if (ok) ok = EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, PROTOTYPE_TAG_BYTES, tag) == 1;
  EVP_CIPHER_CTX_free(ctx);
  return ok ? 0 : 1;
}

int prototypeOpen(const unsigned char *key, const unsigned char *nonce, const unsigned char *aad, int aadLength,
                  const unsigned char *ciphertext, int length, const unsigned char *tag, unsigned char *plaintext) {
  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  unsigned char tagCopy[PROTOTYPE_TAG_BYTES];
  int written = 0;
  int ok = ctx != 0;
  memcpy(tagCopy, tag, PROTOTYPE_TAG_BYTES);
  if (ok) ok = EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), 0, key, nonce) == 1;
  if (ok && aadLength > 0) ok = EVP_DecryptUpdate(ctx, 0, &written, aad, aadLength) == 1;
  if (ok) ok = EVP_DecryptUpdate(ctx, plaintext, &written, ciphertext, length) == 1;
  if (ok) ok = EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, PROTOTYPE_TAG_BYTES, tagCopy) == 1;
  if (ok) ok = EVP_DecryptFinal_ex(ctx, plaintext + written, &written) == 1;
  EVP_CIPHER_CTX_free(ctx);
  return ok ? 0 : 1;
}

int prototypeWrapDataKey(const unsigned char *masterKey, const unsigned char *masterName,
                         const unsigned char *dataKey, unsigned char *record) {
  unsigned char *salt = record + 1;
  unsigned char *wrapped = salt + PROTOTYPE_SALT_BYTES;
  unsigned char *wrapNonce = wrapped + PROTOTYPE_KEY_BYTES;
  unsigned char *wrapTag = wrapNonce + PROTOTYPE_NONCE_BYTES;
  unsigned char *name = wrapTag + PROTOTYPE_TAG_BYTES;
  record[0] = PROTOTYPE_KEY_RECORD_VERSION;
  if (prototypeRandomBytes(salt, PROTOTYPE_SALT_BYTES)) return 1;
  if (prototypeRandomBytes(wrapNonce, PROTOTYPE_NONCE_BYTES)) return 1;
  memcpy(name, masterName, PROTOTYPE_MASTER_NAME_BYTES);
  return prototypeSeal(masterKey, wrapNonce, salt, PROTOTYPE_SALT_BYTES, dataKey, PROTOTYPE_KEY_BYTES, wrapped,
                       wrapTag);
}

int prototypeUnwrapDataKey(const unsigned char *masterKey, const unsigned char *record, unsigned char *dataKey) {
  const unsigned char *salt = record + 1;
  const unsigned char *wrapped = salt + PROTOTYPE_SALT_BYTES;
  const unsigned char *wrapNonce = wrapped + PROTOTYPE_KEY_BYTES;
  const unsigned char *wrapTag = wrapNonce + PROTOTYPE_NONCE_BYTES;
  if (record[0] != PROTOTYPE_KEY_RECORD_VERSION) return 1;
  return prototypeOpen(masterKey, wrapNonce, salt, PROTOTYPE_SALT_BYTES, wrapped, PROTOTYPE_KEY_BYTES, wrapTag,
                       dataKey);
}
