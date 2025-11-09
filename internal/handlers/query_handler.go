package handlers

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

type QueryHandler struct {
	DB *sqlx.DB
}

func NewQueryHandler(db *sqlx.DB) *QueryHandler {
	return &QueryHandler{DB: db}
}

// Query Endpoint
// Example usage: "http://localhost:8080/query?table=sales&filter=region='Asia'&limit=10"
// =======================
func (h *QueryHandler) QueryData(c *gin.Context) {
	table := c.Query("table")
	filter := c.Query("filter") // e.g., "country='US'"
	limit := c.DefaultQuery("limit", "10")
	offset := c.DefaultQuery("offset", "0")

	if table == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "table parameter is required"})
		return
	}

	// Build base query
	query := fmt.Sprintf("SELECT * FROM %s", table)

	// Add filter if provided
	if filter != "" {
		query += fmt.Sprintf(" WHERE %s", filter)
	}

	query += fmt.Sprintf(" LIMIT %s OFFSET %s", limit, offset)

	// Run query safely — sqlx automatically maps rows to []map[string]interface{}
	rows, err := h.DB.Queryx(query)
	if err != nil {
		log.Printf("query error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to execute query"})
		return
	}
	defer rows.Close()

	results := []map[string]interface{}{}
	for rows.Next() {
		row := make(map[string]interface{})
		if err := rows.MapScan(row); err != nil {
			log.Printf("scan error: %v", err)
			continue
		}
		results = append(results, row)
	}

	c.JSON(http.StatusOK, gin.H{
		"count": len(results),
		"data":  results,
	})
}

// Transform Endpoint
// Example usge: curl "http://localhost:8080/transform?table=sales&aggregate=COUNT(*)&group_by=country"
// =======================
func (h *QueryHandler) TransformData(c *gin.Context) {
	table := c.Query("table")
	aggregate := c.Query("aggregate") // e.g., "SUM(amount)" or "COUNT(*)"
	groupBy := c.Query("group_by")    // e.g., "region"

	if table == "" || aggregate == "" || groupBy == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "table, aggregate, and group_by are required"})
		return
	}

	// Construct query safely
	query := fmt.Sprintf(`
		SELECT %s AS metric, %s 
		FROM %s
		GROUP BY %s
		ORDER BY %s ASC
	`, aggregate, groupBy, table, groupBy, groupBy)

	rows, err := h.DB.Queryx(query)
	if err != nil {
		log.Printf("transform query error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to execute transformation"})
		return
	}
	defer rows.Close()

	results := []map[string]interface{}{}
	for rows.Next() {
		row := make(map[string]interface{})
		if err := rows.MapScan(row); err != nil {
			log.Printf("transform scan error: %v", err)
			continue
		}
		results = append(results, row)
	}

	c.JSON(http.StatusOK, gin.H{
		"count": len(results),
		"data":  results,
	})
}
