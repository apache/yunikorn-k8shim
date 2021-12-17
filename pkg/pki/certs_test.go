/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package pki

import (
	"crypto/x509"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestCreateCACertificate(t *testing.T) {
	cert, privateKey, err := GenerateCACertificate(time.Now().AddDate(1, 0, 0))
	assert.NilError(t, err, "generate certificate failed")

	assert.Assert(t, privateKey.PublicKey.Equal(cert.PublicKey), "public keys do not match")

	err = cert.CheckSignatureFrom(cert)
	assert.NilError(t, err, "signature check failed")
}

func TestCreateServerCertificate(t *testing.T) {
	caCert, caPrivateKey, err := GenerateCACertificate(time.Now().AddDate(1, 0, 0))
	assert.NilError(t, err, "generate ca certificate failed")

	cert, privateKey, err := GenerateServerCertificate("example.com", []string{"example.com", "www.example.com"}, caCert, caPrivateKey)
	assert.NilError(t, err, "generate server certificate failed")

	assert.Assert(t, privateKey.PublicKey.Equal(cert.PublicKey), "public keys do not match")

	err = cert.CheckSignatureFrom(caCert)
	assert.NilError(t, err, "signature check failed")
}

func TestEncodeCertificatePem(t *testing.T) {
	cert, _, err := GenerateCACertificate(time.Now().AddDate(1, 0, 0))
	assert.NilError(t, err, "generate certificate failed")

	certPem, err := EncodeCertificatePem(cert)
	assert.NilError(t, err, "encode certificate failed")

	cert2, err := DecodeCertificatePem(certPem)
	assert.NilError(t, err, "decode certificate failed")

	assert.Equal(t, cert.SerialNumber.Int64(), cert2.SerialNumber.Int64(), "wrong serial number")
}

func TestEncodePrivateKeyPem(t *testing.T) {
	_, privateKey, err := GenerateCACertificate(time.Now().AddDate(1, 0, 0))
	assert.NilError(t, err, "generate certificate failed")

	pkPem, err := EncodePrivateKeyPem(privateKey)
	assert.NilError(t, err, "encode private key failed")

	privateKey2, err := DecodePrivateKeyPem(pkPem)
	assert.NilError(t, err, "decode private key failed")

	assert.Assert(t, privateKey.Equal(privateKey2), "private keys do not match")
}

func TestEncodeCertChainPem(t *testing.T) {
	cert1, _, err := GenerateCACertificate(time.Now().AddDate(1, 0, 0))
	assert.NilError(t, err, "generate ca certificate 1 failed")

	cert2, _, err := GenerateCACertificate(time.Now().AddDate(1, 0, 0))
	assert.NilError(t, err, "generate ca certificate 2 failed")

	certsPem, err := EncodeCertChainPem([]*x509.Certificate{cert1, cert2})
	assert.NilError(t, err, "encode ca bundle failed")

	certs, err := DecodeCertChainPem(certsPem)
	assert.NilError(t, err, "decode ca bundle failed")

	assert.Equal(t, 2, len(certs), "Wrong length")
	assert.Equal(t, certs[0].SerialNumber.Int64(), cert1.SerialNumber.Int64(), "ca certificate 1 mismatch")
	assert.Equal(t, certs[1].SerialNumber.Int64(), cert2.SerialNumber.Int64(), "ca certificate 2 mismatch")
}
