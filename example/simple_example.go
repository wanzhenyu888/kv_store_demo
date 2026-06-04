package main

import (
	"fmt"
	"log/slog"
	"gopkg.in/natefinch/lumberjack.v2"

	kv "kv_store_demo"
)

func InitLogger() *slog.Logger {
	writer := &lumberjack.Logger{
		Filename:   "/tmp/kv_store_demo/logs/app.log",
		MaxSize:    200,
		MaxBackups: 7,
		MaxAge:     30,
		Compress:   true,
	}

	handler := slog.NewJSONHandler(
		writer,
		&slog.HandlerOptions{
			Level:     slog.LevelInfo,
			AddSource: true,
		},
	)

	logger := slog.New(handler)
	return logger
}

func main() {
	logger := InitLogger()
	options := kv.Options{
		Dir:          "/tmp/kv_store_demo/data",
		MemTableSize: kv.DefaultMemTableSize,
		Logger:       logger,
	}

	db, err := kv.Open(options)
	if err != nil {
		fmt.Println(err)
	}
	db.Logger.Info("Open DB succeeded.")
}
