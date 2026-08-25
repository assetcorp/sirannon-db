#ifndef SIRANNON_ENCRYPTION_PROTOTYPE_CRYPTO_H
#define SIRANNON_ENCRYPTION_PROTOTYPE_CRYPTO_H

#define PROTOTYPE_PAGE_RESERVED_BYTES 128
#define PROTOTYPE_PLAINTEXT_HEADER_BYTES 100
#define PROTOTYPE_KEY_BYTES 32
#define PROTOTYPE_NONCE_BYTES 12
#define PROTOTYPE_TAG_BYTES 16
#define PROTOTYPE_SALT_BYTES 16
#define PROTOTYPE_MASTER_NAME_BYTES 16
#define PROTOTYPE_KEY_RECORD_VERSION 1
#define PROTOTYPE_KEY_RECORD_BYTES \
  (1 + PROTOTYPE_SALT_BYTES + PROTOTYPE_KEY_BYTES + PROTOTYPE_NONCE_BYTES + PROTOTYPE_TAG_BYTES + \
   PROTOTYPE_MASTER_NAME_BYTES)
#define PROTOTYPE_PAGE_ONE_TAIL_BYTES (PROTOTYPE_NONCE_BYTES + PROTOTYPE_TAG_BYTES + PROTOTYPE_KEY_RECORD_BYTES)

int prototypeRandomBytes(unsigned char *out, int length);
int prototypeSeal(const unsigned char *key, const unsigned char *nonce, const unsigned char *aad, int aadLength,
                  const unsigned char *plaintext, int length, unsigned char *ciphertext, unsigned char *tag);
int prototypeOpen(const unsigned char *key, const unsigned char *nonce, const unsigned char *aad, int aadLength,
                  const unsigned char *ciphertext, int length, const unsigned char *tag, unsigned char *plaintext);
int prototypeWrapDataKey(const unsigned char *masterKey, const unsigned char *masterName,
                         const unsigned char *dataKey, unsigned char *record);
int prototypeUnwrapDataKey(const unsigned char *masterKey, const unsigned char *record, unsigned char *dataKey);

#endif
