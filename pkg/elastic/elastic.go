package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"log/slog"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type Client struct {
	es *elasticsearch.Client
}

const (
	TimeLayout = "20060102150405"
)

func NewClient() (*Client, error) {
	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		return nil, err
	}
	return &Client{es}, nil
}

func (c *Client) Exists(index string, id string) bool {
	res, err := c.es.Exists(index, id)
	if err != nil {
		slog.Error("Error running Exists query",
			"err", err.Error(),
			"index", index,
			"id", id,
		)
		return false
	}
	defer res.Body.Close()
	return res.StatusCode == 200
}

func (c *Client) DeleteIndex(index string) (bool, error) {
	res, err := c.es.Indices.Delete([]string{index})
	if err != nil {
		return false, err
	}
	defer res.Body.Close()
	return res.StatusCode == 200, nil
}

func (c *Client) AddAliasAndDeleteIndex(newIndex string, alias string) (bool, error) {
	// get old indices by alias
	indices, err := c.getIndicesByAlias(alias)
	if err != nil {
		return false, err
	}

	// add alias to newIndex and remove old indices
	return c.addAliasAndDeleteOldIndex(newIndex, alias, indices)
}

func (c *Client) GetDocumentById(index string, id string, v interface{}) error {
	res, err := c.es.Get(index, id)

	if err != nil {
		return err
	}
	defer res.Body.Close()

	err = json.NewDecoder(res.Body).Decode(&v)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) GetDocumentByQuery(ctx context.Context, index string, query map[string]interface{}, v interface{}) (*string, error) {
	queryString, err := json.Marshal(query)
	if err != nil {
		slog.Error("Error encoding query",
			"err", err.Error(),
			"query", query,
		)
		return nil, err
	}

	// get document from elastic
	res, err := c.Search(ctx, index, string(queryString))
	if err != nil {
		slog.Error("Error in elastic GetDocumentByQuery",
			"err", err.Error(),
			"query", string(queryString),
			"index", index,
		)
		return nil, err
	}

	// parse the result
	result := *res
	var id string
	if len(result.Hits.Hits) > 0 {
		hit := result.Hits.Hits[0]
		id = hit.ID
		if hit.Source != nil {
			if err := json.Unmarshal(hit.Source, &v); err != nil {
				slog.Error("Error parsing elastic result hit source ",
					"err", err.Error(),
				)
				return nil, err
			}
		}
	} else {
		return nil, nil
	}

	return &id, nil
}

func (c *Client) GetDocumentField(index string, id string, key string) (interface{}, error) {
	var doc map[string]interface{}
	err := c.GetDocumentById(index, id, &doc)
	if err != nil {
		return nil, err
	}

	src, ok := doc["_source"].(map[string]interface{})
	if !ok {
		return nil, errors.New(`invalid document, no "_source" property`)
	}
	return src[key], nil
}

func (c *Client) Update(index string, id string, v interface{}) error {
	b, err := json.Marshal(&v)
	if err != nil {
		return err
	}

	retryOnConflict := 2
	req := esapi.UpdateRequest{
		Refresh:         "true",
		Index:           strings.ToLower(index),
		DocumentID:      id,
		Body:            bytes.NewReader(b),
		RetryOnConflict: &retryOnConflict,
	}
	res, err := req.Do(context.Background(), c.es)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			slog.Error("Error parsing the response body",
				"err", err.Error(),
				"index", index,
				"id", id,
				"operation", "update",
			)
			return err
		}
		// Print the response status and error information.
		err = fmt.Errorf(
			"failed to update document: [%s] %s: %s",
			res.Status(),
			e["error"].(map[string]interface{})["type"],
			e["error"].(map[string]interface{})["reason"],
		)
		slog.Error("Elastic document update error",
			"err", err.Error(),
			"index", index,
			"id", id,
			"doc", v,
		)
		return err
	}

	return nil
}

func (c *Client) Delete(index string, id string) error {
	req := esapi.DeleteRequest{
		Refresh:    "true",
		Index:      strings.ToLower(index),
		DocumentID: id,
	}
	res, err := req.Do(context.Background(), c.es)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			slog.Error("Error parsing the response body",
				"err", err.Error(),
				"index", index,
				"id", id,
				"operation", "delete",
			)
			return err
		}
		// Print the response status and error information.
		if em, ok := e["error"].(map[string]interface{}); ok {
			err = fmt.Errorf(
				"failed to delete document: [%s] %s: %s",
				res.Status(),
				em["type"],
				em["reason"],
			)
		} else {
			err = fmt.Errorf(
				"failed to delete document: [%s] %s",
				res.Status(),
				e["result"],
			)
		}
		slog.Error("Elastic error deleting document",
			"err", err.Error(),
			"index", index,
			"id", id,
		)
		return err
	}

	return nil
}

func (c *Client) Index(index string, id string, v interface{}) error {
	b, err := json.Marshal(&v)
	if err != nil {
		return err
	}

	req := esapi.IndexRequest{
		Refresh:    "true",
		Index:      strings.ToLower(index),
		DocumentID: id,
		Body:       bytes.NewReader(b),
	}
	res, err := req.Do(context.Background(), c.es)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return nil
}

// https://github.com/elastic/go-elasticsearch/blob/example_bulk/_examples/bulk/bulk.go
func (c *Client) BulkIndex(batchSize int, bulkRequests []*BulkRequest) (result *BulkResult) {
	result = &BulkResult{}

	type bulkResponse struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Index struct {
				ID     string `json:"_id"`
				Result string `json:"result"`
				Status int    `json:"status"`
				Error  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
					Cause  struct {
						Type   string `json:"type"`
						Reason string `json:"reason"`
					} `json:"caused_by"`
				} `json:"error"`
			} `json:"index"`
		} `json:"items"`
	}
	var (
		buf       bytes.Buffer
		numItems  int
		currBatch int
		raw       map[string]interface{}
		blk       *bulkResponse
	)

	count := len(bulkRequests)
	for i, br := range bulkRequests {
		// prepare the metadata payload
		meta := []byte(fmt.Sprintf(`{ "index" : { "_index" : "%s", "_id" : "%s", "_type": "_doc" } }%s`, strings.ToLower(br.Index), br.ID, "\n"))

		// prepare the data payload, encode to JSON
		data, err := json.Marshal(br.Document)
		if err != nil {
			result.AppendError("failed to encode document %s: %s", br.ID, err)
			return result
		}
		// append newline to the data payload
		data = append(data, "\n"...)

		// write meta and data to buffer
		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)

		// when a threshold is reached, execute the Bulk() request with body from buffer
		if i > 0 && i%batchSize == 0 || i == count-1 {
			numItems++

			currBatch = i / batchSize
			if i == count-1 {
				currBatch++
			}

			res, err := c.es.Bulk(bytes.NewReader(buf.Bytes()), c.es.Bulk.WithIndex(strings.ToLower(br.Index)))
			if err != nil {
				result.AppendError("failed to index batch %d: %s", currBatch, err)
				return result
			}

			// if the whole request failed, print error and mark all documents as failed
			if res.IsError() {
				if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
					result.AppendError("failed to parse response body: %s", err)
					return result
				} else {
					result.AppendError("error [%d] %s: %s",
						res.StatusCode,
						raw["error"].(map[string]interface{})["type"],
						raw["error"].(map[string]interface{})["reason"],
					)
				}
			} else {
				// a successful response might still contain errors for particular documents...
				if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
					result.AppendError("failed to parse response body: %s", err)
					return result
				} else {
					for _, d := range blk.Items {
						// ... so for any HTTP status above 201 ...
						if d.Index.Status > 201 {
							result.AppendError("error [%d]: %s %s %s %s",
								d.Index.Status,
								d.Index.Error.Type,
								d.Index.Error.Reason,
								d.Index.Error.Cause.Type,
								d.Index.Error.Cause.Reason,
							)
						} else {
							// ... otherwise increase the success counter
							result.Indexed++
						}
					}
				}
			}

			// Close the response body, to prevent reaching the limit for goroutines or file handles
			res.Body.Close()

			// reset the buffer and items counter
			buf.Reset()
			numItems = 0
		}
	}

	return result
}

func (c *Client) Search(ctx context.Context, index string, query string) (*ElasticSearchResult, error) {
	slog.Debug("Elastic query",
		"query", query,
		"index", index,
	)

	res, err := c.es.Search(
		c.es.Search.WithContext(ctx),
		c.es.Search.WithIndex(index),
		c.es.Search.WithBody(strings.NewReader(query)),
		//es.Search.WithTrackTotalHits(true),
		c.es.Search.WithPretty())
	if err != nil {
		slog.Error("Error getting response",
			"err", err.Error(),
			"query", query,
			"index", index,
		)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			slog.Error("Error parsing the response body",
				"err", err.Error(),
				"query", query,
				"index", index,
			)
			return nil, err
		}
		// Print the response status and error information.
		err = fmt.Errorf(
			"failed to run query: [%s] %s: %s",
			res.Status(),
			e["error"].(map[string]interface{})["type"],
			e["error"].(map[string]interface{})["reason"],
		)
		slog.Error("Elastic search error",
			"err", err.Error(),
			"query", query,
			"index", index,
		)
		return nil, err
	}

	var r ElasticSearchResult

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		slog.Error("Error parsing the response body",
			"err", err.Error(),
		)
		return nil, err
	}

	slog.Debug("Elastic query time:",
		"took", r.Took,
	)

	return &r, nil
}
