package images

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type TokenManager struct {
	mu        sync.RWMutex
	token     string
	expiresAt time.Time
}

type batchTokenResponse struct {
	Result struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expiresAt"`
	} `json:"result"`
	Success bool `json:"success"`
	Errors  []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func (tm *TokenManager) GetValidBatchToken(cflAccount, cflToken string) (string, error) {
	tm.mu.RLock()

	if tm.token != "" && time.Now().Add(5*time.Minute).Before(tm.expiresAt) {
		token := tm.token
		tm.mu.RUnlock()
		return token, nil
	}
	tm.mu.RUnlock()

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.token != "" && time.Now().Add(5*time.Minute).Before(tm.expiresAt) {
		return tm.token, nil
	}

	resp, err := getBatchToken(cflAccount, cflToken)
	if err != nil {
		return "", err
	}

	tm.token = resp.Result.Token
	tm.expiresAt = resp.Result.ExpiresAt

	return tm.token, nil
}

func getBatchToken(cflAccount, apiKey string) (*batchTokenResponse, error) {
	url := fmt.Sprintf(batchTokenUrl, cflAccount)
	resp := &batchTokenResponse{}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %v", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "bedrock-go-client/1.0")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}

	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read http response body: %v", err)
	}

	if res.StatusCode != http.StatusOK {
		err = json.Unmarshal(body, &resp)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal error response: %v", err)
		}
		return nil, fmt.Errorf("error: %d", res.StatusCode)
	}

	// Parse the successful response
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("failed to get batch token: %v", resp.Errors)
	}

	return resp, nil
}
