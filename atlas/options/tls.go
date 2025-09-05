/*
 * This file is part of Atlas-DB.
 *
 * Atlas-DB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Atlas-DB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Atlas-DB. If not, see <https://www.gnu.org/licenses/>.
 *
 */

package options

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"strings"

	"go.uber.org/zap"
)

// GetTLSConfig returns a TLS configuration appropriate for the current environment.
// In production mode, it uses standard certificate verification.
// In development mode, it allows self-signed certificates for localhost/127.0.0.1.
func GetTLSConfig(targetURL string) (*tls.Config, error) {
	if !CurrentOptions.DevelopmentMode {
		// Production mode: use standard certificate verification
		return &tls.Config{}, nil
	}

	// Development mode: create custom verifier for localhost self-signed certs
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		// If we can't parse the URL, fall back to standard verification
		Logger.Warn("Failed to parse URL for TLS config, falling back to standard verification",
			zap.String("url", targetURL), zap.Error(err))
		return &tls.Config{}, nil
	}

	hostname := parsedURL.Hostname()
	isLocalhost := hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1"

	if !isLocalhost {
		// Non-localhost in development mode: use standard verification
		return &tls.Config{}, nil
	}

	// Localhost in development mode: allow self-signed certificates
	return &tls.Config{
		ServerName: hostname,
		VerifyConnection: func(cs tls.ConnectionState) error {
			// Allow self-signed certificates for localhost in development
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("no peer certificates provided")
			}

			cert := cs.PeerCertificates[0]

			// Verify the certificate is for the expected hostname
			if err := cert.VerifyHostname(hostname); err != nil {
				// For localhost, also accept if the certificate has localhost in SAN or CN
				if !isValidLocalhostCert(cert, hostname) {
					return fmt.Errorf("certificate hostname verification failed: %w", err)
				}
			}

			Logger.Debug("Accepting self-signed certificate for localhost in development mode",
				zap.String("hostname", hostname),
				zap.String("cert_subject", cert.Subject.String()))

			return nil
		},
	}, nil
}

// isValidLocalhostCert checks if a certificate is valid for localhost variations
func isValidLocalhostCert(cert *x509.Certificate, hostname string) bool {
	// Check Subject CN
	if strings.Contains(strings.ToLower(cert.Subject.CommonName), "localhost") {
		return true
	}

	// Check SAN entries
	for _, san := range cert.DNSNames {
		if strings.EqualFold(san, hostname) ||
			strings.EqualFold(san, "localhost") {
			return true
		}
	}

	// Check IP SAN entries for 127.0.0.1 and ::1
	for _, ip := range cert.IPAddresses {
		if ip.String() == hostname {
			return true
		}
	}

	return false
}
