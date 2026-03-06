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

	b := &Backend{
		logger:   l,
		cfg:      c,
		sasToken: c.SASToken,
	}

	var (
		containerClient *container.Client
		err             error
	)

	switch {
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

	case c.SASToken != "":
		// Shared Access Signature authentication.
		level.Info(l).Log("msg", "using SAS token for cache operation")
		containerClient, err = container.NewClientWithNoCredential(blobContainerURL(c), nil)
		if err != nil {
			return nil, fmt.Errorf("azure container client, %w", err)
		}

	default:
		// Shared account key authentication.
		if c.AccountKey == "" {
			return nil, errors.New("azure account key is required")
		}
		cred, credErr := azblob.NewSharedKeyCredential(c.AccountName, c.AccountKey)
		if credErr != nil {
			return nil, fmt.Errorf("azure shared key credential, %w", credErr)
		}
		b.sharedKeyCred = cred
		containerClient, err = container.NewClientWithSharedKeyCredential(blobContainerURL(c), cred, nil)
		if err != nil {
			return nil, fmt.Errorf("azure container client, %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	_, err = containerClient.Create(ctx, nil)
	if err != nil {
		// nolint: errorlint
		var respErr *azcore.ResponseError
		if !errors.As(err, &respErr) {
			return nil, fmt.Errorf("azure, unexpected error, %w", err)
		}
		if bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
			level.Error(l).Log("msg", "container already exists", "err", err)
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
			filename := filepath.Base(p)
			cacheKey := filepath.Base(filepath.Dir(p))
			remoteRoot := filepath.Dir(filepath.Dir(p))
			if filename == "" || cacheKey == "" || remoteRoot == "" {
				errCh <- errors.New("missing values")
				return
			}

			blobPath := filepath.Join(cacheKey, filename)
			reqURL, err := b.generateSASTokenWithCDN(remoteRoot, blobPath)
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
			respBody = resp.Body
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
