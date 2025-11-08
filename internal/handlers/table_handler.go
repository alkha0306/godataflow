package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

type TableHandler struct {
	DB *sqlx.DB
}

// TableMetadata represents a record in table_metadata
type TableMetadata struct {
	ID              int       `db:"id" json:"id"`
	TableName       string    `db:"table_name" json:"table_name"`
	TableType       string    `db:"table_type" json:"table_type"`
	RefreshInterval *int      `db:"refresh_interval" json:"refresh_interval,omitempty"`
	CreatedAt       time.Time `db:"created_at" json:"created_at"`
	UpdatedAt       time.Time `db:"updated_at" json:"updated_at"`
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
	TableName       string `json:"table_name" binding:"required"`
	TableType       string `json:"table_type" binding:"required"`
	RefreshInterval *int   `json:"refresh_interval,omitempty"`
}

// CreateTable handles POST /tables
func (h *TableHandler) CreateTable(c *gin.Context) {
	var req CreateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	// Insert into table_metadata
	query := `
		INSERT INTO table_metadata (table_name, table_type, refresh_interval)
		VALUES ($1, $2, $3)
		RETURNING id, table_name, table_type, refresh_interval, created_at, updated_at
	`
	var newTable TableMetadata
	err := h.DB.QueryRowx(query, req.TableName, req.TableType, req.RefreshInterval).StructScan(&newTable)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create table"})
		return
	}

	// Return the new record
	newTable.TableName = req.TableName
	newTable.TableType = req.TableType
	newTable.RefreshInterval = req.RefreshInterval
	c.JSON(http.StatusCreated, newTable)
}
