package plugin

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Verifies the *http.Client the datasource builds (settings.HTTPClientOptions ->
// httpclient) actually honors the configured TLS options and custom headers.
func TestHTTPClientOptions_TLSAndHeaders(t *testing.T) {
	certPEM, keyPEM := selfSignedCert(t)

	settings := backend.DataSourceInstanceSettings{
		JSONData: json.RawMessage(`{
			"tlsSkipVerify": true,
			"tlsAuth": true,
			"tlsAuthWithCACert": true,
			"serverName": "pinot.example.com",
			"httpHeaderName1": "X-Scope-OrgID"
		}`),
		DecryptedSecureJSONData: map[string]string{
			"tlsClientCert":    certPEM,
			"tlsClientKey":     keyPEM,
			"tlsCACert":        certPEM,
			"httpHeaderValue1": "acme",
		},
	}

	opts, err := settings.HTTPClientOptions(context.Background())
	require.NoError(t, err)

	// Custom header is carried into the client options.
	assert.Equal(t, "acme", opts.Header.Get("X-Scope-OrgID"))

	// Inspect the actual *tls.Config the transport will use.
	require.NotNil(t, opts.TLS)
	tlsCfg, err := httpclient.GetTLSConfig(opts)
	require.NoError(t, err)
	assert.True(t, tlsCfg.InsecureSkipVerify, "tlsSkipVerify should disable verification")
	assert.Equal(t, "pinot.example.com", tlsCfg.ServerName)
	assert.Len(t, tlsCfg.Certificates, 1, "client cert (mTLS) should be loaded")
	assert.NotNil(t, tlsCfg.RootCAs, "CA cert should populate the root pool")

	// The client builds without error from these options.
	client, err := httpclient.New(opts)
	require.NoError(t, err)
	assert.NotNil(t, client)
}

// selfSignedCert returns a matching cert/key PEM pair usable for both client
// auth and as a CA cert in the test above.
func selfSignedCert(t *testing.T) (certPEM string, keyPEM string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pinot-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	keyBytes, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)

	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}))
	return certPEM, keyPEM
}
