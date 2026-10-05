package enroll

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/base64"
	"errors"
)

// sealInfo must match SEAL_INFO in the web app's server/seal.ts.
const sealInfo = "ushr-seal-v1"

// NewSealKey returns an ephemeral X25519 key pair for one login or setup flow.
// The CLI sends the public half so the web app stores secrets sealed to it.
func NewSealKey() (*ecdh.PrivateKey, error) {
	return ecdh.X25519().GenerateKey(nil)
}

// SealPublicKey is the wire form of a seal key's public half: base64url, no padding.
func SealPublicKey(priv *ecdh.PrivateKey) string {
	return base64.RawURLEncoding.EncodeToString(priv.PublicKey().Bytes())
}

// OpenSealed decrypts base64url(ephemeral public key || AES-256-GCM ciphertext).
// HKDF-SHA256 over the X25519 shared secret, salted with both public keys,
// yields the 32-byte key and 12-byte nonce; each seal uses a fresh ephemeral key.
func OpenSealed(priv *ecdh.PrivateKey, sealed string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(sealed)
	if err != nil {
		return "", err
	}
	if len(raw) < 32 {
		return "", errors.New("sealed value too short")
	}
	eph, err := ecdh.X25519().NewPublicKey(raw[:32])
	if err != nil {
		return "", err
	}
	shared, err := priv.ECDH(eph)
	if err != nil {
		return "", err
	}
	salt := append(eph.Bytes(), priv.PublicKey().Bytes()...)
	okm, err := hkdf.Key(sha256.New, shared, salt, sealInfo, 44)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(okm[:32])
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	plain, err := gcm.Open(nil, okm[32:], raw[32:], nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
