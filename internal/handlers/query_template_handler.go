package handlers

import (
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

// Struct maps to the saved_queries table
type SavedQuery struct {
	ID          int    `db:"id" json:"id"`
	Name        string `db:"name" json:"name"`
	SQLText     string `db:"sql_text" json:"sql_text"`
	Description string `db:"description" json:"description,omitempty"`
}

// Handler struct
type QueryTemplateHandler struct {
	DB *sqlx.DB
}

func NewQueryTemplateHandler(db *sqlx.DB) *QueryTemplateHandler {
	return &QueryTemplateHandler{DB: db}
}

// List Saved Queries
func (h *QueryTemplateHandler) ListQueries(c *gin.Context) {
	var queries []SavedQuery
	err := h.DB.Select(&queries, "SELECT * FROM saved_queries ORDER BY id ASC")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list saved queries"})
		return
	}
	c.JSON(http.StatusOK, queries)
}

// Save New Query
type CreateQueryRequest struct {
	Name        string `json:"name" binding:"required"`
	SQLText     string `json:"sql_text" binding:"required"`
	Description string `json:"description"`
}

func (h *QueryTemplateHandler) CreateQuery(c *gin.Context) {
	var req CreateQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
		return
	}

	query := `
		INSERT INTO saved_queries (name, sql_text, description)
		VALUES ($1, $2, $3)
		RETURNING id, name, sql_text, description
	`

	var saved SavedQuery
	err := h.DB.QueryRowx(query, req.Name, req.SQLText, req.Description).StructScan(&saved)
	if err != nil {
		log.Printf("insert error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save query"})
		return
	}

	c.JSON(http.StatusCreated, saved)
}

// Run Saved Query by ID
func (h *QueryTemplateHandler) RunSavedQuery(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid query id"})
		return
	}

	var sqlText string
	err = h.DB.Get(&sqlText, "SELECT sql_text FROM saved_queries WHERE id = $1", id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "query not found"})
		return
	}

	// Execute dynamically
	rows, err := h.DB.Queryx(sqlText)
	if err != nil {
		log.Printf("execution error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to run query"})
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
		"id":     id,
		"result": results,
	})
}
