package infra

import (
	"log/slog"
	"os"
)

var Logger *slog.Logger

func InitLogger(logger *slog.Logger) {
	if logger != nil {
		Logger = logger
		return
	}
	
	Logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}
