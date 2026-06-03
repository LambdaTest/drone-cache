package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/meltwater/drone-cache/internal"
	"github.com/meltwater/drone-cache/utils"
)

const (
	// DefaultBlobMaxRetryRequests is the default value for Azure Blob Storage max retry requests.
	DefaultBlobMaxRetryRequests = 4

	defaultBufferSize = 4 * 1024 * 1024
	defaultMaxBuffers = 4
)

// Backend implements storage.Backend for Azure Blob Storage.
type Backend struct {
	logger          log.Logger
	cfg             Config
	containerClient *container.Client
	sharedKeyCred   *azblob.SharedKeyCredential
	sasToken        string
}

// New creates an Azure Blob Storage backend.
func New(l log.Logger, c Config) (*Backend, error) {
	if c.AccountName == "" {
		return nil, errors.New("azure account name is required")
	}

	if c.CDNHost != "" && c.ClientID != "" && c.AccountKey == "" && c.SASToken == "" {
		return nil, errors.New("CDN is not supported with service principal authentication; use account key or SAS token")
	}

	b := &Backend{
		logger:   l,
		cfg:      c,
		sasToken: c.SASToken,
	}

	spnCount := 0
	for _, v := range []bool{c.ClientID != "", c.ClientSecret != "", c.TenantID != ""} {
		if v {
			spnCount++
		}
	}
	if spnCount > 0 && spnCount < 3 && c.AccountKey == "" && c.SASToken == "" {
		return nil, errors.New("all three SPN fields (ClientID, ClientSecret, TenantID) must be provided together")
	}

	var (
		containerClient *container.Client
		err             error
	)

	switch {
	// A SAS token takes precedence over an account key. Foreman historically passes
	// --azure.account-key alongside a per-operation AZURE_SAS_TOKEN, and the SAS token
	// is the credential actually scoped to the operation. Preferring it matches the
	// pre-migration binary and avoids 400 InvalidAuthenticationInfo when the account
	// key is empty or invalid in SAS-based deployments.
	case c.SASToken != "":
		// Shared Access Signature authentication.
		level.Info(l).Log("msg", "using SAS token for cache operation")
		containerClient, err = container.NewClientWithNoCredential(blobContainerURL(c), nil)
		if err != nil {
			return nil, fmt.Errorf("azure container client, %w", err)
		}

	case c.AccountKey != "":
		// Shared account key authentication.
		cred, credErr := azblob.NewSharedKeyCredential(c.AccountName, c.AccountKey)
		if credErr != nil {
			return nil, fmt.Errorf("azure shared key credential, %w", credErr)
		}
		b.sharedKeyCred = cred
		containerClient, err = container.NewClientWithSharedKeyCredential(blobContainerURL(c), cred, nil)
		if err != nil {
			return nil, fmt.Errorf("azure container client, %w", err)
		}

	case c.ClientID != "" && c.ClientSecret != "" && c.TenantID != "":
		// Service Principal authentication.
		level.Info(l).Log("msg", "using service principal for cache operation")
		cred, credErr := azidentity.NewClientSecretCredential(c.TenantID, c.ClientID, c.ClientSecret, nil)
		if credErr != nil {
			return nil, fmt.Errorf("azure spn credential, %w", credErr)
		}
		containerClient, err = container.NewClient(blobContainerURL(c), cred, nil)
		if err != nil {
			return nil, fmt.Errorf("azure container client, %w", err)
		}

	default:
		return nil, errors.New("insufficient azure authentication credentials")
	}

	// A SAS token is scoped to an existing container and lacks account-level
	// permission to create one, so Create() would return 403 AuthorizationFailure.
	// The container is guaranteed to exist (the SAS issuer signed for it).
	//
	// Likewise skip Create() when no explicit container name is configured. A
	// legacy foreman (pre-TE-13070) drops --azure.blob-container-name and passes
	// --remote-root cache instead, so the container rides in as the first segment
	// of the blob key (cache/<orgid>/...) and blobContainerURL() resolves to the
	// account root. Create() against the root returns 400 InvalidQueryParameterValue.
	// The container already exists in that layout, and NewBlockBlobClient(p) routes
	// the leading path segment as the container, so creation is both wrong and unneeded.
	if c.SASToken == "" && c.ContainerName != "" {
		ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
		defer cancel()

		_, err = containerClient.Create(ctx, nil)
		if err != nil {
			var respErr *azcore.ResponseError
			if !errors.As(err, &respErr) {
				return nil, fmt.Errorf("azure, unexpected error, %w", err)
			}
			if bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
				level.Error(l).Log("msg", "container already exists", "err", err)
			} else {
				return nil, fmt.Errorf("azure, create container, %w", err)
			}
		}
	}

	b.containerClient = containerClient
	return b, nil
}

// Get writes downloaded content to the given writer.
func (b *Backend) Get(ctx context.Context, p string, w io.Writer) error {
	errCh := make(chan error)

	go func() {
		defer close(errCh)

		var (
			respBody io.ReadCloser
			err      error
		)

		if b.cfg.CDNHost != "" {
			b.logger.Log("msg", "using cdn host")
			containerName, blobPath := b.cdnContainerAndBlob(p)
			if containerName == "" || blobPath == "" {
				errCh <- errors.New("missing values")
				return
			}

			reqURL, err := b.generateSASTokenWithCDN(containerName, blobPath)
			if err != nil {
				errCh <- fmt.Errorf("sas query params, %w", err)
				return
			}
			retriableClient := utils.GetRetriableClient(b.cfg.MaxRetryRequests, b.cfg.Timeout, nil)
			resp, err := retriableClient.Get(reqURL)
			if err != nil {
				errCh <- fmt.Errorf("get object from cdn, %w", err)
				return
			}
			respBody = resp.Body
		} else {
			resp, err := b.containerClient.NewBlockBlobClient(p).DownloadStream(ctx, nil)
			if err != nil {
				errCh <- fmt.Errorf("get the object, %w", err)
				return
			}
			respBody = resp.NewRetryReader(ctx, &blob.RetryReaderOptions{
				MaxRetries: int32(b.cfg.MaxRetryRequests),
			})
		}

		defer internal.CloseWithErrLogf(b.logger, respBody, "response body, close defer")

		if _, err = io.Copy(w, respBody); err != nil {
			errCh <- fmt.Errorf("copy the object, %w", err)
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		// nolint: wrapcheck
		return ctx.Err()
	}
}

// Put uploads contents of the given reader.
func (b *Backend) Put(ctx context.Context, p string, r io.Reader) error {
	b.logger.Log("msg", "uploading the file with blob", "name", p)

	_, err := b.containerClient.NewBlockBlobClient(p).UploadStream(ctx, r, &blockblob.UploadStreamOptions{
		BlockSize:   defaultBufferSize,
		Concurrency: defaultMaxBuffers,
	})
	if err != nil {
		return fmt.Errorf("put the object, %w", err)
	}

	return nil
}

// Exists checks if path already exists.
func (b *Backend) Exists(ctx context.Context, p string) (bool, error) {
	b.logger.Log("msg", "checking if the object already exists", "name", p)

	_, err := b.containerClient.NewBlockBlobClient(p).GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("check if object exists, %w", err)
	}

	return true, nil
}

// cdnContainerAndBlob resolves the container name and the in-container blob path for
// a CDN request. The CDN fronts blob storage, so the URL must be /<container>/<blob>.
//
// Current layout: an explicit container is configured (--azure.blob-container-name),
// so the whole path is the blob key inside that container.
//
// Legacy layout: no container was configured and the container name rode in as the
// first path segment, because foreman passed --remote-root <container> with an empty
// container. Split that first segment off so historical caches stay reachable, and so
// this binary keeps working when invoked by an older foreman.
func (b *Backend) cdnContainerAndBlob(p string) (containerName, blobPath string) {
	p = strings.TrimPrefix(filepath.ToSlash(p), "/")
	if b.cfg.ContainerName != "" {
		return b.cfg.ContainerName, p
	}
	if container, blob, found := strings.Cut(p, "/"); found {
		return container, blob
	}
	return p, ""
}

// generateSASTokenWithCDN generates a URL pointing at the CDN host, authenticated with a SAS token.
func (b *Backend) generateSASTokenWithCDN(containerName, blobPath string) (string, error) {
	if runtime.GOOS == "windows" {
		containerName = strings.ReplaceAll(containerName, "\\", "/")
		blobPath = strings.ReplaceAll(blobPath, "\\", "/")
	}

	rawURL := url.URL{
		Scheme: "https",
		Host:   b.cfg.CDNHost,
		Path:   "/" + containerName + "/" + blobPath,
	}

	if b.sasToken != "" {
		rawURL.RawQuery = b.sasToken
		return rawURL.String(), nil
	}

	if b.sharedKeyCred == nil {
		return "", errors.New("CDN SAS generation requires shared key credential")
	}

	perms := sas.BlobPermissions{Read: true, List: true}
	queryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(12 * time.Hour),
		ContainerName: containerName,
		BlobName:      blobPath,
		Permissions:   perms.String(),
	}.SignWithSharedKey(b.sharedKeyCred)
	if err != nil {
		return "", fmt.Errorf("generate SAS token, %w", err)
	}

	rawURL.RawQuery = queryParams.Encode()
	return rawURL.String(), nil
}

// blobContainerURL builds the full container URL, appending a SAS token when present.
func blobContainerURL(c Config) string {
	var base string
	if c.Azurite {
		base = fmt.Sprintf("http://%s/%s/%s", c.BlobStorageURL, c.AccountName, c.ContainerName)
	} else {
		base = fmt.Sprintf("https://%s.%s/%s", c.AccountName, c.BlobStorageURL, c.ContainerName)
	}
	if c.SASToken != "" {
		return base + "?" + c.SASToken
	}
	return base
}
