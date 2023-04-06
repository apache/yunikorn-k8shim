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
	"bytes"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/log"
)

const YunikornOrg = "yunikorn.apache.org"

func GenerateCACertificate(notAfter time.Time) (*x509.Certificate, *rsa.PrivateKey, error) {
	caTemplate := &x509.Certificate{
		Subject: pkix.Name{
			Organization: []string{YunikornOrg},
		},
		SerialNumber:          big.NewInt(serialNumber()),
		NotBefore:             time.Now().Add(time.Minute * -5),
		NotAfter:              notAfter,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	return generateCert(caTemplate, nil, nil)
}

func GenerateServerCertificate(cn string, dnsNames []string, signer *x509.Certificate, signerKey *rsa.PrivateKey) (*x509.Certificate, *rsa.PrivateKey, error) {
	certTemplate := &x509.Certificate{
		Subject: pkix.Name{
			CommonName:   cn,
			Organization: []string{YunikornOrg},
		},
		DNSNames:     dnsNames,
		SerialNumber: big.NewInt(serialNumber()),
		NotBefore:    time.Now().Add(time.Minute * -5),
		NotAfter:     time.Now().AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	return generateCert(certTemplate, signer, signerKey)
}

func EncodeCertificatePem(cert *x509.Certificate) (*[]byte, error) {
	certPem := new(bytes.Buffer)
	err := pem.Encode(certPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Raw,
	})
	if err != nil {
		log.Logger().Error("Unable to encode certificate", zap.Error(err))
		return nil, err
	}
	data := certPem.Bytes()
	return &data, nil
}

func EncodeCertChainPem(certs []*x509.Certificate) (*[]byte, error) {
	certsPem := new(bytes.Buffer)
	for _, cert := range certs {
		err := pem.Encode(certsPem, &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: cert.Raw,
		})
		if err != nil {
			log.Logger().Error("Unable to encode certificate", zap.Error(err))
			return nil, err
		}
	}

	data := certsPem.Bytes()
	return &data, nil
}

func DecodeCertChainPem(certsPem *[]byte) ([]*x509.Certificate, error) {
	certs := make([]*x509.Certificate, 0)
	data := *certsPem
	for {
		block, rest := pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			log.Logger().Error("Unable to decode certificate")
			return nil, errors.New("pki: unable to decode certificate")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			log.Logger().Error("Unable to parse certificate", zap.Error(err))
			return nil, err
		}
		certs = append(certs, cert)
		data = rest
	}

	return certs, nil
}

func DecodeCertificatePem(certPem *[]byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(*certPem)
	if block == nil || block.Type != "CERTIFICATE" {
		log.Logger().Error("Unable to decode certificate")
		return nil, errors.New("pki: unable to decode certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		log.Logger().Error("Unable to parse certificate", zap.Error(err))
		return nil, err
	}
	return cert, err
}

func EncodePrivateKeyPem(privateKey *rsa.PrivateKey) (*[]byte, error) {
	pkPem := new(bytes.Buffer)
	err := pem.Encode(pkPem, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	if err != nil {
		log.Logger().Error("Unable to encode private key", zap.Error(err))
		return nil, err
	}
	data := pkPem.Bytes()
	return &data, nil
}

func DecodePrivateKeyPem(privateKeyPem *[]byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(*privateKeyPem)
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		log.Logger().Error("Unable to decode private key")
		return nil, errors.New("pki: unable to decode private key")
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		log.Logger().Error("Unable to parse private key", zap.Error(err))
		return nil, err
	}
	return privateKey, err
}

func serialNumber() int64 {
	return time.Now().UnixNano() / int64(time.Microsecond)
}

func generateCert(certTemplate *x509.Certificate, signer *x509.Certificate, signerKey *rsa.PrivateKey) (*x509.Certificate, *rsa.PrivateKey, error) {
	// private key
	privateKey, err := rsa.GenerateKey(cryptorand.Reader, 4096)
	if err != nil {
		log.Logger().Error("Unable to generate private key", zap.Error(err))
		return nil, nil, err
	}

	// default to self-signed
	caKey := privateKey
	certSigner := certTemplate

	// use provided signer if present
	if signer != nil && signerKey != nil {
		caKey = signerKey
		certSigner = signer
	}

	// create certificate
	certBytes, err := x509.CreateCertificate(cryptorand.Reader, certTemplate, certSigner, &privateKey.PublicKey, caKey)
	if err != nil {
		log.Logger().Error("Unable to create certificate", zap.Error(err))
		return nil, nil, err
	}

	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		log.Logger().Error("Unable to parse certificate", zap.Error(err))
		return nil, nil, err
	}

	return cert, privateKey, nil
}
