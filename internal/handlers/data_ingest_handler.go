package handlers

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

type DataIngestHandler struct {
	DB *sqlx.DB
}

func NewDataIngestHandler(db *sqlx.DB) *DataIngestHandler {
	return &DataIngestHandler{DB: db}
}

// IngestData handles POST /ingest/:table_name
func (h *DataIngestHandler) IngestData(c *gin.Context) {
	tableName := c.Param("table_name")
	if tableName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing table name"})
		return
	}

	// Verify that the table exists in metadata
	var exists bool
	val_err := h.DB.Get(&exists, "SELECT EXISTS (SELECT 1 FROM table_metadata WHERE table_name=$1)", tableName)
	if val_err != nil {
		log.Printf("metadata check error: %v", val_err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check metadata"})
		return
	}
	if !exists {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("table '%s' is not registered", tableName)})
		return
	}

	// Parse JSON body (accepts array or single record)
	var records []map[string]interface{}
	if err := c.ShouldBindJSON(&records); err != nil {
		// If a single object was sent, wrap it in an array
		var single map[string]interface{}
		if err2 := c.ShouldBindJSON(&single); err2 != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON"})
			return
		}
		records = append(records, single)
	}

	if len(records) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no data provided"})
		return
	}

	// Dynamically build INSERT query
	cols := make([]string, 0, len(records[0]))
	valPlaceholders := make([]string, 0, len(records))
	valArgs := []interface{}{}

	for col := range records[0] {
		cols = append(cols, col)
	}

	for i, record := range records {
		placeholders := []string{}
		for j, col := range cols {
			valArgs = append(valArgs, record[col])
			placeholders = append(placeholders, fmt.Sprintf("$%d", i*len(cols)+j+1))
		}
		valPlaceholders = append(valPlaceholders, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES %s`,
		tableName,
		strings.Join(cols, ", "),
		strings.Join(valPlaceholders, ", "),
	)

	// Execute query safely using placeholders
	_, err := h.DB.Exec(query, valArgs...)
	if err != nil {
		log.Printf("insert error: table=%s err=%v", tableName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to insert data", "details": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":    "data inserted successfully",
		"table_name": tableName,
		"row_count":  len(records),
		"columns":    cols,
	})
}
