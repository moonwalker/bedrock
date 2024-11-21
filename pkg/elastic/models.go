package elastic

import "encoding/json"

type BulkRequest struct {
	Index    string
	ID       string
	Document interface{}
}

type BulkResult struct {
	Errors  []error
	Indexed int
}

type ElasticResultHit struct {
	Index  string          `json:"_index"`
	Type   string          `json:"_type"`
	ID     string          `json:"_id"`
	Score  float64         `json:"_score"`
	Source json.RawMessage `json:"_source"`
	Fields json.RawMessage `json:"fields,omitempty"`
	Sort   []int64         `json:"sort,omitempty"`
}

type ElasticResultTotal struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type ElasticResultHits struct {
	Total    ElasticResultTotal `json:"total"`
	MaxScore float64            `json:"max_score"`
	Hits     []ElasticResultHit `json:"hits"`
}

type ElasticResultShards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}

type ElasticSearchResult struct {
	Took         int                 `json:"took"`
	TimedOut     bool                `json:"timed_out"`
	Shards       ElasticResultShards `json:"_shards"`
	Hits         ElasticResultHits   `json:"hits"`
	Aggregations json.RawMessage     `json:"aggregations"`
}

type ElasticAggregateBucket struct {
	DocCount int         `json:"doc_count"`
	Key      interface{} `json:"key"`
}
