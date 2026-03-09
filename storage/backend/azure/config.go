package azure

import "time"

// Config is a structure to store Azure backend configuration.
type Config struct {
	AccountName      string
	AccountKey       string
	SASToken         string
	ContainerName    string
	BlobStorageURL   string
	CDNHost          string
	Azurite          bool
	MaxRetryRequests int
	Timeout          time.Duration

	// ClientID, ClientSecret, and TenantID enable Service Principal (SPN) authentication
	// when all three are provided. Leave empty to use SASToken or AccountKey instead.
	ClientID     string
	ClientSecret string
	TenantID     string
}
