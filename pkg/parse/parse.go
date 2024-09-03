package parse

import (
	"fmt"
	"strconv"
	"time"
)

func ParseString(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func ParseInt64(v interface{}) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int32:
		return int64(t)
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case float32:
		return int64(t)
	case string:
		i, _ := strconv.ParseInt(t, 10, 64)
		return i
	}
	return 0
}

func ParseInt(v interface{}) int {
	return int(ParseInt64(v))
}

func ParseFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int64:
		return float64(t)
	case int32:
		return float64(t)
	case int:
		return float64(t)
	case string:
		i, _ := strconv.ParseFloat(t, 10)
		return i
	case []byte:
		i, _ := strconv.ParseFloat(string(t), 10)
		return i
	}
	return 0
}

func ParseFloat32(v interface{}) float32 {
	return float32(ParseFloat(v))
}

func FloatToBytes(f float64) []byte {
	return []byte(fmt.Sprintf("%.2f", f))
}

func ParseBool(v interface{}) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		b, _ := strconv.ParseBool(t)
		return b
	}
	return false
}

func ParseDate(v interface{}) time.Time {
	switch t := v.(type) {
	case time.Time:
		return t
	case string:
		d, _ := time.Parse(time.RFC3339Nano, t)
		return d
	}
	return time.Time{}
}

func ParseUnix(v string) int64 {
	d, _ := time.Parse(time.RFC3339Nano, v)
	return d.Unix()
}

func ParseHappened(s string) int64 {
	v, u := parseWhen(s, true)
	return calcTime(v, u)
}

func ParseScheduled(s string) int64 {
	v, u := parseWhen(s, false)
	return calcTime(v, u)
}

func parseWhen(s string, past bool) (int, string) {
	var v int                     // value
	var u string                  // unit
	fmt.Sscanf(s, "%d%s", &v, &u) // format ("2d", etc.)
	if past {
		return -v, u
	}
	return v, u
}

func calcTime(v int, u string) int64 {
	now := time.Now().UTC()
	if u == "s" {
		return now.Add(time.Duration(v) * time.Second).Unix()
	}
	if u == "m" {
		return now.Add(time.Duration(v) * time.Minute).Unix()
	}
	if u == "h" {
		return now.Add(time.Duration(v) * time.Hour).Unix()
	}
	rounded := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	if u == "D" {
		return rounded.AddDate(0, 0, v).Unix()
	}
	if u == "W" {
		return rounded.AddDate(0, 0, 7*v).Unix()
	}
	if u == "M" {
		return rounded.AddDate(0, v, 0).Unix()
	}
	if u == "Y" {
		return rounded.AddDate(v, 0, 0).Unix()
	}
	return 0
}
