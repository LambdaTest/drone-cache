package azure

import (
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
)

// TestBlobContainerURL verifies URL construction for all config combinations.
func TestBlobContainerURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "plain https",
			cfg: Config{
				AccountName:    "myaccount",
				BlobStorageURL: "blob.core.windows.net",
				ContainerName:  "mycontainer",
			},
			want: "https://myaccount.blob.core.windows.net/mycontainer",
		},
		{
			name: "with SAS token appended",
			cfg: Config{
				AccountName:    "myaccount",
				BlobStorageURL: "blob.core.windows.net",
				ContainerName:  "mycontainer",
				SASToken:       "sv=2020-08-04&ss=b",
			},
			want: "https://myaccount.blob.core.windows.net/mycontainer?sv=2020-08-04&ss=b",
		},
		{
			name: "azurite http",
			cfg: Config{
				AccountName:    "devstoreaccount1",
				BlobStorageURL: "127.0.0.1:10000",
				ContainerName:  "testcontainer",
				Azurite:        true,
			},
			want: "http://127.0.0.1:10000/devstoreaccount1/testcontainer",
		},
		{
			name: "azurite with SAS token",
			cfg: Config{
				AccountName:    "devstoreaccount1",
				BlobStorageURL: "127.0.0.1:10000",
				ContainerName:  "testcontainer",
				Azurite:        true,
				SASToken:       "sig=abc123",
			},
			want: "http://127.0.0.1:10000/devstoreaccount1/testcontainer?sig=abc123",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := blobContainerURL(tc.cfg)
			if got != tc.want {
				t.Errorf("blobContainerURL() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestNewValidation verifies that New() rejects invalid configurations early.
func TestNewValidation(t *testing.T) {
	t.Parallel()

	logger := log.NewNopLogger()

	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name:    "missing account name",
			cfg:     Config{},
			wantErr: "azure account name is required",
		},
		{
			// CDN+SPN with no fallback auth: rejected.
			name: "CDN with SPN and no fallback auth rejected",
			cfg: Config{
				AccountName:  "myaccount",
				CDNHost:      "mycdn.azureedge.net",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				TenantID:     "tenant-id",
			},
			wantErr: "CDN is not supported with service principal authentication",
		},
		{
			name: "partial SPN - ClientID and TenantID only, no fallback",
			cfg: Config{
				AccountName: "myaccount",
				ClientID:    "client-id",
				TenantID:    "tenant-id",
			},
			wantErr: "all three SPN fields (ClientID, ClientSecret, TenantID) must be provided together",
		},
		{
			name: "partial SPN - ClientID only, no fallback",
			cfg: Config{
				AccountName: "myaccount",
				ClientID:    "client-id",
			},
			wantErr: "all three SPN fields (ClientID, ClientSecret, TenantID) must be provided together",
		},
		{
			name: "partial SPN - ClientSecret and TenantID only, no fallback",
			cfg: Config{
				AccountName:  "myaccount",
				ClientSecret: "client-secret",
				TenantID:     "tenant-id",
			},
			wantErr: "all three SPN fields (ClientID, ClientSecret, TenantID) must be provided together",
		},
		{
			name: "no credentials at all",
			cfg: Config{
				AccountName:    "myaccount",
				BlobStorageURL: "blob.core.windows.net",
				ContainerName:  "mycontainer",
				Timeout:        5 * time.Second,
			},
			wantErr: "insufficient azure authentication credentials",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(logger, tc.cfg)
			if err == nil {
				t.Fatalf("New() expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("New() error = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestNewPartialSPNWithFallbackAuth verifies that partial SPN fields are silently
// ignored when a higher-priority auth (AccountKey or SASToken) is present.
func TestNewPartialSPNWithFallbackAuth(t *testing.T) {
	t.Parallel()

	logger := log.NewNopLogger()

	spnErr := "all three SPN fields (ClientID, ClientSecret, TenantID) must be provided together"
	cdnSPNErr := "CDN is not supported with service principal authentication"

	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "partial SPN + AccountKey uses AccountKey",
			cfg: Config{
				AccountName: "myaccount",
				AccountKey:  "c29tZWtleQ==",
				ClientID:    "client-id",
				Timeout:     5 * time.Second,
			},
		},
		{
			name: "partial SPN + SASToken uses SASToken",
			cfg: Config{
				AccountName:    "myaccount",
				BlobStorageURL: "blob.core.windows.net",
				ContainerName:  "mycontainer",
				SASToken:       "sv=2020-08-04&ss=b",
				ClientID:       "client-id",
				Timeout:        5 * time.Second,
			},
		},
		{
			name: "CDN + partial SPN + AccountKey uses AccountKey (CDN+SPN check skipped)",
			cfg: Config{
				AccountName: "myaccount",
				AccountKey:  "c29tZWtleQ==",
				CDNHost:     "mycdn.azureedge.net",
				ClientID:    "client-id",
				Timeout:     5 * time.Second,
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(logger, tc.cfg)
			// We expect an error (no real Azure endpoint), but NOT a validation error.
			if err != nil && (strings.Contains(err.Error(), spnErr) || strings.Contains(err.Error(), cdnSPNErr)) {
				t.Errorf("New() returned validation error unexpectedly: %v", err)
			}
		})
	}
}

