package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
)

// PreviewSourceHandler GET /preview_source?url=...
// Returns a small preview of the JSON at the provided URL.
// Truncates arrays to max 10 elements and objects to max 20 keys.
func PreviewSourceHandler(c *gin.Context) {
	rawURL := c.Query("url")
	if rawURL == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "url query param required"})
		return
	}
	// validate URL
	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil || !(parsed.Scheme == "http" || parsed.Scheme == "https") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid url"})
		return
	}

	// fetch with timeout
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot create request", "details": err.Error()})
		return
	}

	cli := &http.Client{}
	resp, err := cli.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to fetch url", "details": err.Error()})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.JSON(http.StatusBadGateway, gin.H{"error": "upstream returned non-2xx", "status": resp.Status})
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // limit 1MB
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to read response", "details": err.Error()})
		return
	}

	// decode JSON into interface{}
	var parsedJSON interface{}
	dec := json.NewDecoder((io.NopCloser(io.LimitReader(resp.Body, 0)))) // noop - we'll use body
	_ = dec                                                              // avoid unused (we use body directly below)

	if err := json.Unmarshal(body, &parsedJSON); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "response is not valid JSON", "details": err.Error()})
		return
	}

	// Build truncated preview
	preview := buildPreview(parsedJSON)

	c.JSON(http.StatusOK, gin.H{
		"preview": preview,
	})
}

// buildPreview truncates arrays/objects for preview.
func buildPreview(v interface{}) interface{} {
	const maxArray = 10
	const maxObject = 20

	switch t := v.(type) {
	case []interface{}:
		n := len(t)
		limit := maxArray
		if n < limit {
			limit = n
		}
		out := make([]interface{}, 0, limit)
		for i := 0; i < limit; i++ {
			out = append(out, buildPreview(t[i])) // recursively preview elements
		}
		return out
	case map[string]interface{}:
		out := map[string]interface{}{}
		i := 0
		for k, val := range t {
			if i >= maxObject {
				out["__truncated"] = "(more keys omitted)"
				break
			}
			out[k] = buildPreview(val)
			i++
		}
		return out
	default:
		// primitive (string, float64, bool, nil) — return as-is
		return v
	}
}
