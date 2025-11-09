package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/alkha0306/godataflow/internal/config"
	"github.com/alkha0306/godataflow/internal/db"
	"github.com/alkha0306/godataflow/internal/handlers"
	"github.com/gin-gonic/gin"
)

func main() {
	// 1. Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config load error: %v", err)
	}

	// 2. Connect to DB
	database, err := db.Connect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db connect error: %v", err)
	}
	defer database.Close()

	// Run DB migrations
	if err := db.RunMigrations(database); err != nil {
		log.Fatalf("migrations error: %v", err)
	}
	log.Println("All migrations applied")

	// 3. Setup Gin router
	router := gin.Default()

	// Health check
	router.GET("/health", handlers.HealthHandler)

	// Table management APIs
	tableHandler := handlers.NewTableHandler(database)
	router.GET("/tables", tableHandler.ListTables)
	router.POST("/tables", tableHandler.CreateTable)
	router.DELETE("/tables/:name", tableHandler.DeleteTable)

	// Data ingestion API
	dataIngestHandler := handlers.NewDataIngestHandler(database)
	router.POST("/ingest/:name", dataIngestHandler.IngestData)

	// Query and Transform data API
	queryHandler := handlers.NewQueryHandler(database)
	router.GET("/query", queryHandler.QueryData)
	router.GET("/transform", queryHandler.TransformData)

	// saved queries mgmt API
	queryTemplateHandler := handlers.NewQueryTemplateHandler(database)
	router.GET("/queries", queryTemplateHandler.ListQueries)
	router.POST("/queries", queryTemplateHandler.CreateQuery)
	router.GET("/queries/run/:id", queryTemplateHandler.RunSavedQuery)

	// 4. Start server with graceful shutdown
	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	// Run server in goroutine (non-blocking)
	go func() {
		log.Printf("Server running on port %s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server start error: %v", err)
		}
	}()

	// Graceful shutdown on Ctrl+C
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit // wait for signal

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited cleanly")
}
