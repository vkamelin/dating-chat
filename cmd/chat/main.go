package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"meet-you-chat/internal/auth"
	"meet-you-chat/internal/chat"
	"meet-you-chat/internal/config"
	"meet-you-chat/internal/db"
	"meet-you-chat/internal/events"
	httpx "meet-you-chat/internal/http"
	"meet-you-chat/internal/logging"
	"meet-you-chat/internal/redisx"
	"meet-you-chat/internal/ws"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	logger := logging.New()

	sqlDB, err := db.Open(cfg)
	if err != nil {
		logger.Error("open mysql failed", "error", err)
		os.Exit(1)
	}
	defer sqlDB.Close()

	redisClient, err := redisx.New(cfg)
	if err != nil {
		logger.Error("open redis failed", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	authenticator, err := auth.New(cfg, sqlDB)
	if err != nil {
		logger.Error("init auth failed", "error", err)
		os.Exit(1)
	}

	repo := chat.NewRepository(sqlDB)
	hub := ws.NewHub(logger)
	service := chat.NewService(cfg, repo, hub, logger)
	consumer := events.NewConsumer(cfg, service, logger, redisClient)

	mux := http.NewServeMux()
	health := httpx.NewHealthHandler(sqlDB, redisClient)
	mux.HandleFunc("GET /health", health.Health)
	mux.HandleFunc("GET /ready", health.Ready)
	mux.HandleFunc("GET /ws", ws.NewHandler(cfg, authenticator, service, hub, redisClient, logger).ServeHTTP)

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- consumer.Run(ctx)
	}()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http server started", "addr", cfg.HTTPAddr)
		serverErr <- server.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		logger.Info("shutdown requested", "signal", sig.String())
	case err := <-consumerErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("redis consumer stopped", "error", err)
		}
		cancel()
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server stopped", "error", err)
		}
		cancel()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	cancel()
	hub.CloseAll()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", "error", err)
	}

	if err := consumer.Close(); err != nil {
		logger.Error("consumer close failed", "error", err)
	}

	select {
	case err := <-consumerErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("consumer exit", "error", err)
		}
	default:
	}

	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server exit", "error", err)
		}
	default:
	}
}
