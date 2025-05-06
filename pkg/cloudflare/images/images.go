package images

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"time"

	"log/slog"

	"github.com/gabriel-vasile/mimetype"
)

const (
	apiurl      = "https://api.cloudflare.com/client/v4/accounts/%s/images/v1" // cloudflare api url
	imageCDNFmt = "https://imagedelivery.net/%s/%s/public"
)

type uploadImageParams struct {
	File     *bytes.Buffer
	URL      string
	Name     string
	Path     string
	Metadata map[string]string
}

type errorResponse struct {
	Errors []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Messages []interface{} `json:"messages"`
	Result   interface{}   `json:"result"`
	Success  bool          `json:"success"`
}

type imageDetailResponse struct {
	Errors   []interface{} `json:"errors"`
	Messages []interface{} `json:"messages"`
	Result   struct {
		Filename string `json:"filename"`
		ID       string `json:"id"`
		Meta     struct {
			Key string `json:"key"`
		} `json:"meta"`
		RequireSignedURLs bool      `json:"requireSignedURLs"`
		Uploaded          time.Time `json:"uploaded"`
		Variants          []string  `json:"variants"`
	} `json:"result"`
	Success bool `json:"success"`
}

type ImageUploadInfo struct {
	Filename string `json:"filename"`
	ImageUrl string `json:"imageurl"`
	MimeType string `json:"mimetype"`
}

// GET https://api.cloudflare.com/client/v4/accounts/{account_identifier}/images/v1/{identifier}
func Exists(cflAccount, cflImagesToken, imageID string) (bool, error) {
	url := fmt.Sprintf(apiurl, cflAccount) + "/" + imageID

	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", cflImagesToken))
	req.Header.Add("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("http request failed: %w", err)
	}

	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return false, fmt.Errorf("failed to read http response body: %w", err)
	}
	resp := &imageDetailResponse{}
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal error response: %w", err)
	}

	return resp.Success, nil
}

func Upload(cflAccount, cflAccountHash, cflImagesToken, id string, imageContent []byte, imageURL string) (*ImageUploadInfo, error) {
	url := fmt.Sprintf(apiurl, cflAccount)

	form := map[string]string{"id": id}
	p := uploadImageParams{
		URL:      imageURL,
		Name:     id,
		Metadata: form,
	}
	if imageContent != nil {
		p.File = bytes.NewBuffer(imageContent)
	}
	ct, payload, err := createForm(p)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	slog.Debug("uploading image to cfl",
		"imageID", id,
		"contentType", ct,
		"name", p.Name,
	)
	var resp interface{}
	err = req(cflImagesToken, http.MethodPost, url, payload, resp, ct)
	if err != nil {
		return nil, fmt.Errorf("failed to upload image: %w, url:%s, contentType:%s", err, url, ct)
	}

	res := &ImageUploadInfo{
		Filename: p.Name,
		ImageUrl: fmt.Sprintf(imageCDNFmt, cflAccountHash, p.Name),
	}
	if imageContent != nil {
		mime := mimetype.Detect(imageContent)
		if mime != nil {
			res.MimeType = mime.String()
		}
	}

	return res, nil
}

func Delete(cflAccount, cflImagesToken, id string) error {
	url := fmt.Sprintf(apiurl, cflAccount) + "/" + id

	slog.Debug("deleting image from cfl",
		"imageID", id,
		"url", url,
	)

	err := req(cflImagesToken, http.MethodDelete, url, nil, nil, "")
	if err != nil {
		return fmt.Errorf("failed to delete image: %w", err)
	}

	return nil
}

func req(cflToken, method, url string, payload io.Reader, resp interface{}, contentType string) error {
	req, _ := http.NewRequest(method, url, payload)
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", cflToken))
	if len(contentType) == 0 {
		req.Header.Add("Content-Type", "application/json")
	} else {
		req.Header.Add("Content-Type", contentType)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}

	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("failed to read http response body: %w", err)
	}

	if res.StatusCode != http.StatusOK {
		resp := &errorResponse{}
		err = json.Unmarshal(body, &resp)
		if err != nil {
			return fmt.Errorf("failed to unmarshal error response: %w", err)
		}
		err = fmt.Errorf("error: %d", res.StatusCode)
		if len(resp.Errors) > 0 {
			err = fmt.Errorf("%w: %s", err, resp.Errors[0].Message)
		}
		return err
	}

	if resp != nil {
		err = json.Unmarshal(body, &resp)
		if err != nil {
			return fmt.Errorf("failed to unmarshal response body: %w", err)
		}
	}

	return nil
}

func createForm(p uploadImageParams) (string, io.Reader, error) {
	body := new(bytes.Buffer)
	mp := multipart.NewWriter(body)
	defer mp.Close()
	for key, val := range p.Metadata {
		mp.WriteField(key, val)
	}

	if len(p.Path) > 0 {
		file, err := os.Open(p.Path)
		if err != nil {
			return "", nil, err
		}
		defer file.Close()
		part, err := mp.CreateFormFile("file", p.Path)
		if err != nil {
			return "", nil, err
		}
		io.Copy(part, file)
	}
	if p.File != nil {
		part, err := mp.CreateFormFile("file", p.Name)
		if err != nil {
			return "", nil, err
		}
		io.Copy(part, p.File)
	}
	if len(p.URL) > 0 {
		mp.WriteField("url", p.URL)
	}
	return mp.FormDataContentType(), body, nil
}
