package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

type RefreshLogsHandler struct {
	DB *sqlx.DB
}

func NewRefreshLogsHandler(db *sqlx.DB) *RefreshLogsHandler {
	return &RefreshLogsHandler{DB: db}
}

// GET /refresh_logs/:table
func (h *RefreshLogsHandler) GetLogs(c *gin.Context) {
	table := c.Param("table")
	if table == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "table name required"})
		return
	}

	type LogEntry struct {
		ID        int    `db:"id" json:"id"`
		TableName string `db:"table_name" json:"table_name"`
		Status    string `db:"status" json:"status"`
		Message   string `db:"message" json:"message"`
		CreatedAt string `db:"created_at" json:"created_at"`
	}

	var logs []LogEntry
	err := h.DB.Select(&logs,
		`SELECT id, table_name, status, message, created_at 
		 FROM refresh_logs 
		 WHERE table_name = $1
		 ORDER BY created_at DESC
		 LIMIT 100`,
		table,
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch logs", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, logs)
}
