package kv

import (
	"fmt"
	"time"

	"github.com/imroc/req/v3"
	"github.com/moonwalker/bedrock/pkg/env"
)

var (
	accountID     = env.Must("CFL_ACCOUNT_ID")
	kvNamespaceID = env.Must("CFL_KV_NS_ID")
	workersToken  = env.Must("CFL_WORKERS_TOKEN")
	baseURL       = fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/storage/kv/namespaces/%s", accountID, kvNamespaceID)
	client        = req.C().SetBaseURL(baseURL)
)

func Delete(key string) error {
	_, err := client.R().
		SetBearerAuthToken(workersToken).
		SetHeader("content-type", "application/json").
		Delete(fmt.Sprintf("/values/%s", key))
	return err
}

func Write(key string, value string) error {
	_, err := client.R().
		SetBearerAuthToken(workersToken).
		SetHeader("content-type", "multipart/form-data").
		SetFormData(map[string]string{
			"metadata": fmt.Sprintf(`{"timestamp": %s}`, time.Now().UTC()),
			"value":    value,
		}).
		Put(fmt.Sprintf("/values/%s", key))
	return err
}
