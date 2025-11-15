package etl

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// ETLProcessor contains DB and helper methods for ETL.
type ETLProcessor struct {
	DB *sqlx.DB
}

// NewETLProcessor creates an instance.
func NewETLProcessor(db *sqlx.DB) *ETLProcessor {
	return &ETLProcessor{DB: db}
}

// -----------------------------
// FetchData
// Fetches URL and returns a slice of row maps.
// Supports either object or array JSON responses.
// -----------------------------
func (e *ETLProcessor) FetchData(url string) ([]map[string]interface{}, error) {
	if url == "" {
		return nil, errors.New("empty data source url")
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("http get failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("http status %d: %s", resp.StatusCode, string(body))
	}

	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()

	// Try to decode into either array or object
	var raw interface{}
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("json decode failed: %w", err)
	}

	switch v := raw.(type) {
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				out = append(out, m)
			} else {
				// try to convert scalars -> wrap in map?
				return nil, errors.New("array items are not objects")
			}
		}
		return out, nil
	case map[string]interface{}:
		return []map[string]interface{}{v}, nil
	default:
		return nil, errors.New("unexpected JSON type: expected object or array of objects")
	}
}

// -----------------------------
// ValidatePayload
// Ensures incoming keys exist in table and tries to normalize values to appropriate Go types.
// Returns validated/normalized rows (may convert strings->numbers, parse timestamps, etc.)
// -----------------------------
func (e *ETLProcessor) ValidatePayload(tableName string, rows []map[string]interface{}) ([]map[string]interface{}, error) {
	if err := sanitizeIdentifier(tableName); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if len(rows) == 0 {
		return nil, errors.New("no rows to validate")
	}

	// Load column metadata
	colQuery := `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
	`
	type colInfo struct {
		ColumnName string `db:"column_name"`
		DataType   string `db:"data_type"`
	}
	var cols []colInfo
	if err := e.DB.Select(&cols, colQuery, tableName); err != nil {
		return nil, fmt.Errorf("failed to load table columns: %w", err)
	}

	colTypeMap := map[string]string{}
	for _, c := range cols {
		colTypeMap[c.ColumnName] = strings.ToLower(c.DataType)
	}

	// Validate and coerce
	validated := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out := map[string]interface{}{}
		for k, v := range r {
			// Skip unknown columns (optionally you may choose to error instead)
			colType, ok := colTypeMap[k]
			if !ok {
				// drop unknown column
				continue
			}

			normalized, err := coerceValue(colType, v)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", k, err)
			}
			out[k] = normalized
		}
		if len(out) == 0 {
			// nothing matched known columns
			continue
		}
		validated = append(validated, out)
	}

	return validated, nil
}

// coerceValue attempts to convert an arbitrary interface{} to a DB-friendly Go type based on dataType
func coerceValue(dataType string, val interface{}) (interface{}, error) {
	// handle json.Number -> decide numeric type
	if jn, ok := val.(json.Number); ok {
		// try integer first
		if i64, err := jn.Int64(); err == nil {
			return i64, nil
		}
		if f64, err := jn.Float64(); err == nil {
			return f64, nil
		}
		return jn.String(), nil
	}

	switch v := val.(type) {
	case float64:
		// JSON numbers decode to float64 by default
		// if integer-like and dataType contains int -> return int64
		if strings.Contains(dataType, "int") {
			return int64(v), nil
		}
		return v, nil
	case int, int32, int64:
		return v, nil
	case bool:
		return v, nil
	case string:
		// try parse timestamp if dataType contains timestamp or date
		if strings.Contains(dataType, "timestamp") || strings.Contains(dataType, "date") {
			// attempt several common formats
			if t, err := tryParseTime(v); err == nil {
				return t.Format(time.RFC3339), nil
			}
			// let DB attempt parsing if we can't parse
			return v, nil
		}
		// For numeric DB types, attempt parse
		if strings.Contains(dataType, "int") {
			if i, err := parseStringToInt(v); err == nil {
				return i, nil
			}
		}
		if strings.Contains(dataType, "double") || strings.Contains(dataType, "numeric") || strings.Contains(dataType, "real") || strings.Contains(dataType, "float") {
			if f, err := parseStringToFloat(v); err == nil {
				return f, nil
			}
		}
		if strings.Contains(dataType, "boolean") {
			l := strings.ToLower(v)
			if l == "true" || l == "t" || l == "1" {
				return true, nil
			}
			if l == "false" || l == "f" || l == "0" {
				return false, nil
			}
		}
		return v, nil
	case nil:
		return nil, nil
	default:
		// for nested maps / arrays: JSON raw -> marshal to string
		rv := reflect.ValueOf(val)
		if rv.Kind() == reflect.Map || rv.Kind() == reflect.Slice {
			enc, err := json.Marshal(val)
			if err != nil {
				return nil, fmt.Errorf("cannot marshal complex value: %w", err)
			}
			return string(enc), nil
		}
		return v, nil
	}
}

func parseStringToInt(s string) (int64, error) {
	s = strings.TrimSpace(s)
	var i int64
	_, err := fmt.Sscan(s, &i)
	if err != nil {
		return 0, err
	}
	return i, nil
}

func parseStringToFloat(s string) (float64, error) {
	s = strings.TrimSpace(s)
	var f float64
	_, err := fmt.Sscan(s, &f)
	if err != nil {
		return 0, err
	}
	return f, nil
}

func tryParseTime(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC3339Nano,
		time.RFC1123,
	}
	for _, fmtStr := range formats {
		if t, err := time.Parse(fmtStr, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized time format")
}

// -----------------------------
// TransformPayload
// - Flatten simple nested maps (one-level) using dot notation: {"a":{"b":1}} -> {"a.b":1}
// - Convert any timestamp-like strings to RFC3339 strings
// -----------------------------
func (e *ETLProcessor) TransformPayload(rows []map[string]interface{}) []map[string]interface{} {
	outRows := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out := map[string]interface{}{}
		for k, v := range r {
			// if v is a map[string]interface{} flatten one level
			if m, ok := v.(map[string]interface{}); ok {
				for k2, v2 := range m {
					out[fmt.Sprintf("%s.%s", k, k2)] = v2
				}
			} else {
				// if string and looks like timestamp, normalize later in coerceValue
				out[k] = v
			}
		}
		outRows = append(outRows, out)
	}
	return outRows
}

// -----------------------------
// InsertRows
// Insert rows into table (1-by-1 inside a transaction).
// Uses parameterized queries to avoid SQL injection.
// -----------------------------
func (e *ETLProcessor) InsertRows(tableName string, rows []map[string]interface{}) (int, error) {
	if err := sanitizeIdentifier(tableName); err != nil {
		return 0, fmt.Errorf("invalid table name: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	tx, err := e.DB.Beginx()
	if err != nil {
		return 0, fmt.Errorf("begin tx failed: %w", err)
	}
	defer func() {
		// if still active, rollback
		_ = tx.Rollback()
	}()

	inserted := 0
	for _, row := range rows {
		cols := make([]string, 0, len(row))
		placeholders := make([]string, 0, len(row))
		values := make([]interface{}, 0, len(row))
		i := 1
		for k, v := range row {
			cols = append(cols, fmt.Sprintf("\"%s\"", k)) // quote column names
			placeholders = append(placeholders, fmt.Sprintf("$%d", i))
			values = append(values, v)
			i++
		}
		// keep column order stable by sorting? Not necessary but deterministic not guaranteed for map
		// To make deterministic, build cols/values from slice rather than map iteration order
		// For simplicity: we assume row map insertion order is acceptable for now.

		query := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)", tableName, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
		if _, err := tx.Exec(query, values...); err != nil {
			return inserted, fmt.Errorf("insert failed: %w", err)
		}
		inserted++
	}

	if err := tx.Commit(); err != nil {
		return inserted, fmt.Errorf("tx commit failed: %w", err)
	}
	return inserted, nil
}

// -----------------------------
// WriteRefreshLog
// -----------------------------
func (e *ETLProcessor) WriteRefreshLog(tableName, status, message string) error {
	_, err := e.DB.Exec(`INSERT INTO refresh_logs (table_name, status, message) VALUES ($1, $2, $3)`, tableName, status, message)
	return err
}

// -----------------------------
// UpdateMetadataStatus
// Updates last_refresh_success/_error and status column in table_metadata
// -----------------------------
func (e *ETLProcessor) UpdateMetadataStatus(tableName, status string, errorMsg *string) error {
	if err := sanitizeIdentifier(tableName); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	if status == "OK" {
		_, err := e.DB.Exec(`UPDATE table_metadata SET last_refresh_success = NOW(), last_refresh_error = NULL, status = $1, updated_at = NOW() WHERE table_name = $2`, status, tableName)
		return err
	}
	// ERROR status
	_, err := e.DB.Exec(`UPDATE table_metadata SET last_refresh_error = $1, status = $2, updated_at = NOW() WHERE table_name = $3`, errorMsg, status, tableName)
	return err
}

// -----------------------------
// Helpers
// -----------------------------

var identifierRE = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// sanitizeIdentifier ensures the table identifier only contains safe characters
func sanitizeIdentifier(s string) error {
	if s == "" {
		return errors.New("empty identifier")
	}
	if !identifierRE.MatchString(s) {
		return errors.New("identifier contains invalid characters (allowed: A-Z a-z 0-9 _)")
	}
	return nil
}
