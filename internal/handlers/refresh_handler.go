package handlers

import (
	"fmt"
	"net/http"

	"github.com/alkha0306/godataflow/internal/etl"
	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

type RefreshHandler struct {
	DB  *sqlx.DB
	ETL *etl.ETLProcessor
}

func NewRefreshHandler(db *sqlx.DB) *RefreshHandler {
	return &RefreshHandler{
		DB:  db,
		ETL: etl.NewETLProcessor(db),
	}
}

// POST /refresh/:table
func (h *RefreshHandler) ManualRefresh(c *gin.Context) {
	table := c.Param("table")
	if table == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "table name required"})
		return
	}

	// 1. Load table metadata (get data_source_url)
	var meta struct {
		DataSourceURL *string `db:"data_source_url"`
	}

	err := h.DB.Get(&meta,
		`SELECT data_source_url FROM table_metadata WHERE table_name = $1`,
		table,
	)

	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "table not found", "details": err.Error()})
		return
	}
	if meta.DataSourceURL == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "table missing data_source_url"})
		return
	}

	// 2. FETCH
	rows, err := h.ETL.FetchData(*meta.DataSourceURL)
	if err != nil {
		h.ETL.WriteRefreshLog(table, "ERROR", err.Error())
		msg := err.Error()
		h.ETL.UpdateMetadataStatus(table, "ERROR", &msg)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 3. TRANSFORM
	rows = h.ETL.TransformPayload(rows)

	// 4. VALIDATE
	validRows, err := h.ETL.ValidatePayload(table, rows)
	if err != nil {
		h.ETL.WriteRefreshLog(table, "ERROR", err.Error())
		msg := err.Error()
		h.ETL.UpdateMetadataStatus(table, "ERROR", &msg)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 5. INSERT
	count, err := h.ETL.InsertRows(table, validRows)
	if err != nil {
		msg := err.Error()
		h.ETL.WriteRefreshLog(table, "ERROR", msg)
		h.ETL.UpdateMetadataStatus(table, "ERROR", &msg)
		c.JSON(http.StatusInternalServerError, gin.H{"error": msg})
		return
	}

	// 6. SUCCESS
	h.ETL.WriteRefreshLog(table, "OK", fmt.Sprintf("Inserted %d rows", count))
	h.ETL.UpdateMetadataStatus(table, "OK", nil)

	c.JSON(http.StatusOK, gin.H{
		"table":         table,
		"status":        "OK",
		"inserted_rows": count,
		"message":       "Refresh completed successfully",
	})
}
