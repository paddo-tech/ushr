package enroll

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

// seal mirrors seal() in the web app's server/seal.ts.
func seal(t *testing.T, pub *ecdh.PublicKey, plain string) string {
	t.Helper()
	eph, err := ecdh.X25519().GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	shared, err := eph.ECDH(pub)
	if err != nil {
		t.Fatal(err)
	}
	salt := append(eph.PublicKey().Bytes(), pub.Bytes()...)
	okm, err := hkdf.Key(sha256.New, shared, salt, sealInfo, 44)
	if err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(okm[:32])
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(gcm.Seal(eph.PublicKey().Bytes(), okm[32:], []byte(plain), nil))
}

func TestSealRoundTrip(t *testing.T) {
	priv, err := NewSealKey()
	if err != nil {
		t.Fatal(err)
	}
	sealed := seal(t, priv.PublicKey(), "enrollment-token")
	got, err := OpenSealed(priv, sealed)
	if err != nil || got != "enrollment-token" {
		t.Fatalf("open: %q, %v", got, err)
	}
	other, err := NewSealKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSealed(other, sealed); err == nil {
		t.Fatal("a different key opened the seal")
	}
}

// TestOpenWebSeal opens a value that server/seal.ts sealed to the private key 0x07 * 32.
func TestOpenWebSeal(t *testing.T) {
	priv, err := ecdh.X25519().NewPrivateKey(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if got := SealPublicKey(priv); got != "E75P6uryBMf9M1j8nAByGIHRdCeBKCJ-xnTzf3_pe20" {
		t.Fatalf("public key %q", got)
	}
	got, err := OpenSealed(priv, "R19AwTD4eVeynmFRz6CwaWNzlWvn7fuuoqiupyRrozYRu7HL6OEuV62CDxI7a5pF93n3oU7yAEcOfyAwoA")
	if err != nil || got != "interop-token" {
		t.Fatalf("open: %q, %v", got, err)
	}
}
