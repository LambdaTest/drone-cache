package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/meltwater/drone-cache/internal"
	"github.com/meltwater/drone-cache/utils"
)

const (
	// DefaultBlobMaxRetryRequests Default value for Azure Blob Storage Max Retry Requests.
	DefaultBlobMaxRetryRequests = 4

	defaultBufferSize = 4 * 1024 * 1024
	defaultMaxBuffers = 4
)

// Backend implements sotrage.Backend for Azure Blob Storage.
type Backend struct {
	logger              log.Logger
	httpClient          *http.Client
	cfg                 Config
	containerURL        azblob.ContainerURL
	sasToken            string
	sharedKeyCredential azblob.StorageAccountCredential
}

// New creates an AzureBlob backend.
func New(l log.Logger, c Config) (*Backend, error) {
	var credential azblob.Credential

	var err error
	b := &Backend{
		logger:     l,
		cfg:        c,
		httpClient: http.DefaultClient,
	}
	if c.AccountName == "" {
		return nil, errors.New("azure account name is required")
	}
	// 2. Create a default request pipeline using your storage account name and account key.
	if c.SASToken != "" {
		level.Info(l).Log("msg", "using token for cache operation")
		credential = azblob.NewAnonymousCredential()
	} else if c.AccountKey == "" {
		return nil, errors.New("azure account key is required")
	} else if c.AccountKey != "" {
		credential, err = azblob.NewSharedKeyCredential(c.AccountName, c.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("azure, invalid credentials, %w", err)
		}
		var ok bool
		b.sharedKeyCredential, ok = credential.(azblob.StorageAccountCredential)
		if !ok {
			return nil, errors.New("azure, invalid credentials")
		}
	}

	// 3. Azurite has different URL pattern than production Azure Blob Storage.
	var blobURL *url.URL
	if c.Azurite {
		blobURL, err = url.Parse(fmt.Sprintf("http://%s/%s/%s", c.BlobStorageURL, c.AccountName, c.ContainerName))
	} else {
		blobURL, err = url.Parse(fmt.Sprintf("https://%s.%s/%s", c.AccountName, c.BlobStorageURL, c.ContainerName))
	}

	if c.SASToken != "" {
		blobURL.RawQuery = c.SASToken
	}

	if err != nil {
		level.Error(l).Log("msg", "can't create url with : "+err.Error())
	}

	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: 30 * time.Minute,
		},
	})
	containerURL := azblob.NewContainerURL(*blobURL, pipeline)

	// 4. Always creating new container, it will throw error if it already exists.
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		// nolint: errorlint
		ret, ok := err.(azblob.StorageError)
		if !ok {
			return nil, fmt.Errorf("azure, unexpected error, %w", err)
		}

		if ret.ServiceCode() == "ContainerAlreadyExists" {
			level.Error(l).Log("msg", "container already exists", "err", err)
		}
	}
	b.containerURL = containerURL
	b.sasToken = c.SASToken
	return b, nil
}

// Get writes downloaded content to the given writer.
func (b *Backend) Get(ctx context.Context, p string, w io.Writer) error {
	errCh := make(chan error)

	go func() {
		defer close(errCh)
		var respBody io.ReadCloser
		var err error

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
			defer internal.CloseWithErrLogf(b.logger, respBody, "response body, close defer")

		} else {
			blobURL := b.containerURL.NewBlockBlobURL(p)
			resp, err := blobURL.Download(ctx, 0, azblob.CountToEnd,
				azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
			if err != nil {
				errCh <- fmt.Errorf("get the object, %w", err)

				return
			}

			respBody = resp.Body(azblob.RetryReaderOptions{MaxRetryRequests: b.cfg.MaxRetryRequests})
			defer internal.CloseWithErrLogf(b.logger, respBody, "response body, close defer")
		}

		_, err = io.Copy(w, respBody)
		if err != nil {
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

	blobURL := b.containerURL.NewBlockBlobURL(p)
	if _, err := azblob.UploadStreamToBlockBlob(ctx, r, blobURL,
		azblob.UploadStreamToBlockBlobOptions{
			BufferSize: defaultBufferSize,
			MaxBuffers: defaultMaxBuffers,
		},
	); err != nil {
		return fmt.Errorf("put the object, %w", err)
	}

	return nil
}

// Exists checks if path already exists.
func (b *Backend) Exists(ctx context.Context, p string) (bool, error) {
	b.logger.Log("msg", "checking if the object already exists", "name", p)

	blobURL := b.containerURL.NewBlockBlobURL(p)

	get, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return false, fmt.Errorf("check if object exists, %w", err)
	}

	return get.StatusCode() == http.StatusOK, nil
}

// Exists checks if path already exists.
func (b *Backend) generateSASTokenWithCDN(containerName, blobPath string) (string, error) {
	if runtime.GOOS == "windows" {
		containerName = strings.Replace(containerName, "\\", "/", -1) // Replace backslashes with forward slashes
		blobPath = strings.Replace(blobPath, "\\", "/", -1)           // Replace backslashes with forward slashes
	}
	parts := azblob.BlobURLParts{
		Scheme:        "https",
		Host:          b.cfg.CDNHost,
		ContainerName: containerName,
		BlobName:      blobPath,
	}
	var rawURL url.URL
	if b.sasToken == "" {
		sasDefaultSignature := azblob.BlobSASSignatureValues{
			Protocol:      azblob.SASProtocolHTTPS,
			ExpiryTime:    time.Now().UTC().Add(12 * time.Hour),
			ContainerName: containerName,
			BlobName:      blobPath,
			Permissions:   azblob.BlobSASPermissions{Read: true, List: true}.String(),
		}
		sasQueryParams, err := sasDefaultSignature.NewSASQueryParameters(b.sharedKeyCredential)
		if err != nil {
			return "", err
		}
		parts.SAS = sasQueryParams
		rawURL = parts.URL()
	} else {
		rawURL = parts.URL()
		rawURL.RawQuery = b.sasToken
	}

	return rawURL.String(), nil
}
