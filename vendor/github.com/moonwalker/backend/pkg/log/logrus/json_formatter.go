package logrus

import (
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
)

const RFC3339Milli = "2006-01-02T15:04:05.999Z07:00"
const defaultTimestampFormat = RFC3339Milli

// Default key names for the default fields
const (
	FieldKeyTime  = "timestamp"
	FieldKeyMsg   = "message"
	FieldKeyLevel = "level"
)

// FieldMap allows customization of the key names for default fields.
type fieldKey string
type FieldMap map[fieldKey]string

func (f FieldMap) resolve(key fieldKey) string {
	if k, ok := f[key]; ok {
		return k
	}
	return string(key)
}

// JSONFormatter formats logs into parsable json
type JSONFormatterEx struct {
	// FieldMap allows users to customize the names of keys for default fields.
	FieldMap FieldMap
}

// Format renders a single log entry
func (f *JSONFormatterEx) Format(entry *logrus.Entry) ([]byte, error) {
	data := make(logrus.Fields, len(entry.Data)+3)
	for k, v := range entry.Data {
		switch v := v.(type) {
		case error:
			data[k] = v.Error()
		default:
			data[k] = v
		}
	}

	data[f.FieldMap.resolve(FieldKeyTime)] = entry.Time.UTC().Format(RFC3339Milli)
	data[f.FieldMap.resolve(FieldKeyMsg)] = entry.Message
	data[f.FieldMap.resolve(FieldKeyLevel)] = entry.Level.String()

	serialized, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal fields to JSON, %v", err)
	}

	return append(serialized, '\n'), nil
}
