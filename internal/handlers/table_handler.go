package handlers

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

type TableHandler struct {
	DB *sqlx.DB
}

// TableMetadata represents a record in table_metadata
type TableMetadata struct {
	ID                 int        `db:"id" json:"id"`
	TableName          string     `db:"table_name" json:"table_name"`
	TableType          string     `db:"table_type" json:"table_type"`
	RefreshInterval    *int       `db:"refresh_interval" json:"refresh_interval,omitempty"`
	DataSourceURL      *string    `db:"data_source_url" json:"data_source_url,omitempty"`
	LastRefreshSuccess *time.Time `db:"last_refresh_success" json:"last_refresh_success,omitempty"`
	LastRefreshError   *string    `db:"last_refresh_error" json:"last_refresh_error,omitempty"`
	Status             string     `db:"status" json:"status"`
	CreatedAt          time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt          time.Time  `db:"updated_at" json:"updated_at"`
}

func NewTableHandler(db *sqlx.DB) *TableHandler {
	return &TableHandler{DB: db}
}

// ListTables handles GET /tables
func (h *TableHandler) ListTables(c *gin.Context) {
	var tables []TableMetadata
	err := h.DB.Select(&tables, "SELECT * FROM table_metadata ORDER BY id ASC")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch tables"})
		return
	}
	c.JSON(http.StatusOK, tables)
}

// CreateTableRequest is the expected payload for POST /tables
type CreateTableRequest struct {
	TableName       string            `json:"table_name" binding:"required"`
	TableType       string            `json:"table_type" binding:"required"`
	RefreshInterval *int              `json:"refresh_interval,omitempty"`
	Columns         map[string]string `json:"columns" binding:"required"` // key=name, value=type (e.g. "id":"SERIAL PRIMARY KEY", "value":"FLOAT")
}

// CreateTable handles POST /tables
func (h *TableHandler) CreateTable(c *gin.Context) {
	var req CreateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	// Basic validation on columns
	if len(req.Columns) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least one column required"})
		return
	}

	columnDefs := []string{}
	for name, colType := range req.Columns {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", name, colType))
	}
	createStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s);`, req.TableName, strings.Join(columnDefs, ", "))

	// Execute table creation
	if _, err := h.DB.Exec(createStmt); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create table", "details": err.Error()})
		return
	}

	// Insert into table_metadata
	insert_query := `
		INSERT INTO table_metadata (table_name, table_type, refresh_interval)
		VALUES ($1, $2, $3)
		RETURNING id, table_name, table_type, refresh_interval, created_at, updated_at
	`
	var meta TableMetadata
	err := h.DB.QueryRowx(insert_query, req.TableName, req.TableType, req.RefreshInterval).StructScan(&meta)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create table"})
		return
	}

	// Return the new record
	c.JSON(http.StatusCreated, meta)
}

// DeleteTable handles tables/:name it grabs table name from url params drops the actual table and deletes metadata
func (h *TableHandler) DeleteTable(c *gin.Context) {
	tableName := c.Param("name")
	if tableName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "table name required"})
		return
	}

	// Drop the table itself
	dropStmt := fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName)
	if _, err := h.DB.Exec(dropStmt); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to drop table", "details": err.Error()})
		return
	}

	// Remove from metadata
	if _, err := h.DB.Exec(`DELETE FROM table_metadata WHERE table_name = $1;`, tableName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to remove metadata", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "table deleted", "table": tableName})
}

// GET /tables/:name/columns
func (h *TableHandler) GetTableColumns(c *gin.Context) {
	tableName := c.Param("name")

	query := `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position;
	`

	var cols []struct {
		ColumnName string `db:"column_name" json:"column_name"`
		DataType   string `db:"data_type" json:"data_type"`
	}

	if err := h.DB.Select(&cols, query, tableName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch columns"})
		return
	}
	c.JSON(http.StatusOK, cols)
}

// PUT /tables/:name/config
func (h *TableHandler) UpdateTableConfig(c *gin.Context) {
	table := c.Param("name")

	var req struct {
		DataSourceURL   *string `json:"data_source_url"`
		RefreshInterval *int    `json:"refresh_interval"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	_, err := h.DB.Exec(`
		UPDATE table_metadata
		SET data_source_url = COALESCE($1, data_source_url),
		    refresh_interval = COALESCE($2, refresh_interval),
		    updated_at = NOW()
		WHERE table_name = $3
	`, req.DataSourceURL, req.RefreshInterval, table)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update metadata", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "config updated",
		"table":   table,
	})
}
