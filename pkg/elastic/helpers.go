package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"log/slog"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func String(v interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", errors.New("type assertion failed, not string")
	}
	return s, nil
}

func (bres *BulkResult) AppendError(format string, a ...interface{}) {
	bres.Errors = append(bres.Errors, fmt.Errorf(format, a...))
}

func (c *Client) getIndicesByAlias(alias string) ([]string, error) {
	// get index by alias
	req := esapi.IndicesGetAliasRequest{
		Name: []string{alias},
	}
	resA, err := req.Do(context.Background(), c.es)
	if err != nil {
		slog.Error("Error fetching index(es) by alias",
			"err", err.Error(),
			"alias", alias,
		)
		return nil, err
	}
	defer resA.Body.Close()

	found := true
	if resA.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(resA.Body).Decode(&e); err != nil {
			slog.Error("Error parsing the response body",
				"err", err.Error(),
				"alias", alias,
			)
			return nil, err
		}
		// Print the response status and error information.
		if e["status"] == float64(404) {
			found = false
			slog.Warn("Alias was not found",
				"alias", alias,
			)
		} else {
			err = fmt.Errorf(
				"failed to get index(es) by alias: [%s] %s",
				resA.Status(),
				e["error"],
			)
			slog.Error("Elastic IndicesGetAliasRequest failed",
				"err", err.Error(),
				"alias", alias,
			)

			return nil, err
		}
	}

	indices := make([]string, 0)
	// at least one index was found by alias
	if found {
		var r map[string]interface{}
		if err := json.NewDecoder(resA.Body).Decode(&r); err != nil {
			slog.Error("Error parsing the response body",
				"err", err.Error(),
			)
			return nil, err
		}

		// indices with alias
		for index, _ := range r {
			// check index name format to avoid deleting game round index or view
			_, err := time.Parse(TimeLayout, strings.ReplaceAll(index, fmt.Sprintf("%s_", alias), ""))
			if err == nil {
				indices = append(indices, index)
			}
		}
	}

	return indices, nil
}

func (c *Client) addAliasAndDeleteOldIndex(newIndex string, alias string, indices []string) (bool, error) {
	// create query
	query := getUpdateAliasQuery(newIndex, alias, indices)
	queryString, err := json.Marshal(query)
	if err != nil {
		slog.Error("Error encoding query",
			"err", err.Error(),
			"query", query,
		)
		return false, err
	}

	// execute query
	resU, err := c.es.Indices.UpdateAliases(strings.NewReader(string(queryString)))
	if err != nil {
		slog.Error("Error adding alias and deleting index(es)",
			"err", err.Error(),
			"query", string(queryString),
		)
		return false, err
	}
	defer resU.Body.Close()

	if resU.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(resU.Body).Decode(&e); err != nil {
			slog.Error("Error parsing the response body",
				"err", err.Error(),
				"alias", alias,
				"newIndex", newIndex,
				"indices", indices,
			)
			return false, err
		}
		// Print the response status and error information.
		err = fmt.Errorf(
			"failed to add alias to index: [%s] %s",
			resU.Status(),
			e["error"],
		)
		slog.Error("Elastic IndicesGetAliasRequest failed",
			"err", err.Error(),
			"alias", alias,
			"newIndex", newIndex,
			"indices", indices,
		)

		return false, err
	}

	return true, nil
}

// queries
func getUpdateAliasQuery(newIndex string, alias string, indices []string) map[string]interface{} {
	return map[string]interface{}{
		"actions": getUpdateAliasesActions(newIndex, alias, indices),
	}
}

func getUpdateAliasesActions(newIndex string, alias string, indices []string) []map[string]interface{} {
	res := make([]map[string]interface{}, 0)

	res = append(res, addAlias(newIndex, alias))

	for _, ind := range indices {
		res = append(res, removeIndex(ind))
	}

	return res
}

func addAlias(index string, alias string) map[string]interface{} {
	return map[string]interface{}{
		"add": map[string]interface{}{
			"index": index,
			"alias": alias,
		},
	}
}

func removeIndex(index string) map[string]interface{} {
	return map[string]interface{}{
		"remove_index": map[string]interface{}{
			"index": index,
		},
	}
}
