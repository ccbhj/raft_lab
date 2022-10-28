package logging

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
)

var loggers sync.Map

type Level int

const (
	LevelError Level = iota
	LevelInfo
	LevelDebug
)
const (
	RequestIdCtxKey = "request_id"
)

type Logger struct {
	*log.Logger
	level   Level
	ctxKeys []string
	ctx     context.Context
	out     io.Writer
}

func GetLogger(ctx context.Context, key string) *Logger {
	l, in := loggers.Load(key)
	if !in {
		panic(fmt.Sprintf("logger for %s not init", key))
	}

	logger := &(*(l.(*Logger)))
	logger.ctx = ctx
	return logger
}

func InitLogger(out io.Writer, key string, ctxKeys []string) *Logger {
	logger := &log.Logger{}

	logger.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	logger.SetPrefix(fmt.Sprintf("[%s]", key))
	logger.SetOutput(out)

	_, found := loggers.Load(key)
	if found {
		panic(fmt.Sprintf("logger for %s already init", key))
	}
	l := &Logger{Logger: logger, level: GetLogLevel(key), ctxKeys: ctxKeys, out: out}
	loggers.Store(key, l)
	return l
}

func (l *Logger) Debug(f string, args ...interface{}) {
	l.print(LevelDebug, f, args...)
}

func (l *Logger) Info(f string, args ...interface{}) {
	l.print(LevelInfo, f, args...)
}

func (l *Logger) Error(f string, args ...interface{}) {
	l.print(LevelError, f, args...)
}

func (l *Logger) GetOutput() io.Writer {
	return l.out
}

func (l *Logger) print(level Level, f string, args ...interface{}) {
	if level > l.level {
		return
	}
	_, file, lineno, _ := runtime.Caller(2)
	file = path.Base(file)
	s := SprintfColor(level.GetColor(), "%s:%d: [%s]%s", file, lineno, level.String(), l.logContent(f, args...))
	l.Printf(s)
}

func (l *Logger) logContent(f string, args ...interface{}) string {
	prefixes := make([]string, 0, len(l.ctxKeys))
	for _, key := range l.ctxKeys {
		v := l.ctx.Value(key)
		if v == nil {
			continue
		}
		prefixes = append(prefixes, fmt.Sprintf("%s=%v", key, v))
	}
	prefix := strings.Join(prefixes, "|")

	return fmt.Sprintf(prefix + "|" + fmt.Sprintf(f, args...))
}

func GetLogLevel(key string) Level {
	envLevel := os.Getenv(key + "_LOG_LEVEL")
	switch strings.ToUpper(envLevel) {
	case "ERROR":
		return LevelError
	case "INFO":
		return LevelInfo
	case "DEBUG":
		return LevelDebug
	}

	return LevelInfo
}

var levelColor = [...]Color{
	LevelError: ColorRED,
	LevelInfo:  ColorYEL,
	LevelDebug: ColorGRN,
}

var levelStr = [...]string{
	LevelError: "ERROR",
	LevelInfo:  "INFO",
	LevelDebug: "DEBUG",
}

func (l Level) String() string {
	if l < LevelError || l > LevelDebug {
		panic("invalid log level")
	}
	return levelStr[l]
}

func (l Level) GetColor() Color {
	if l < LevelError || l > LevelDebug {
		panic("invalid log level")
	}
	return levelColor[l]
}

func GetRequestIdFromCtx(ctx context.Context) string {
	reqId := ctx.Value(RequestIdCtxKey)
	if reqId != nil {
		return reqId.(string)
	}
	return ""
}
