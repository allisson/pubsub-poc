package pubsubpoc

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	logger *zap.Logger
)

// NewProductionEncoderConfig returns an opinionated EncoderConfig for
// production environments.
func NewProductionEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.EpochTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

// NewProductionConfig is a reasonable production logging configuration.
// Logging is enabled at InfoLevel and above.
//
// It uses a JSON encoder, writes to standard error, and enables sampling.
// Stacktraces are automatically included on logs of ErrorLevel and above.
func NewProductionConfig() zap.Config {
	return zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "json",
		EncoderConfig:    NewProductionEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
}

// NewDevelopmentEncoderConfig returns an opinionated EncoderConfig for
// development environments.
func NewDevelopmentEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:        "time",
		LevelKey:       "severity",
		NameKey:        "N",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

// NewDevelopmentConfig is a reasonable development logging configuration.
// Logging is enabled at DebugLevel and above.
//
// It enables development mode (which makes DPanicLevel logs panic), uses a
// console encoder, writes to standard error, and disables sampling.
// Stacktraces are automatically included on logs of WarnLevel and above.
func NewDevelopmentConfig() zap.Config {
	return zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:      true,
		Encoding:         "json",
		EncoderConfig:    NewDevelopmentEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
}

// NewLogger returns a zap logger based on LOG_CONFIG envvar
// if LOG_CONFIG=="production" returns zap.NewProduction() else returns zap.NewDevelopment()
func NewLogger() (logger *zap.Logger, err error) {
	logConfig := os.Getenv("LOG_CONFIG")

	if logConfig == "production" {
		logger, err = NewProductionConfig().Build()
	} else {
		logger, err = NewDevelopmentConfig().Build()
	}

	return logger, err
}

func init() {
	logger, _ = NewLogger()
	// nolint:errcheck
	defer logger.Sync()
}
