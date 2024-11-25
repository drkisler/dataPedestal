package service

import (
	"fmt"
	logCtl "github.com/drkisler/dataPedestal/universal/logAdmin/control"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var LogWriter *TLogWriter

// TLogWriter 日志写入器
type TLogWriter struct {
	logCtl *logCtl.TSysLogControl
	logDir string
	mu     *sync.Mutex
}

func NewLogWriter(strLocate string) *TLogWriter {
	var logger logCtl.TSysLogControl
	var mutex sync.Mutex

	exe, _ := os.Executable()
	dir := filepath.Join(filepath.Dir(exe), "logs")
	// 确保日志目录存在
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal().Err(err).Msg("无法创建日志目录")
	}
	logger.LogLocate = strLocate

	return &TLogWriter{logCtl: &logger, mu: &mutex, logDir: dir}
}

func ConsoleInfo(info string) {
	exe, _ := os.Executable()
	dir := filepath.Join(filepath.Dir(exe), "logs")
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal().Err(err).Msg("无法创建日志目录")
	}

	logFileName := fmt.Sprintf("%s/%s%s", dir, time.Now().Format("2006-01-02"), ".log")
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal().Err(err).Msg("无法打开日志文件")
	}
	defer func() {
		_ = logFile.Close()
	}()
	log.Logger = zerolog.New(logFile).With().Timestamp().Logger()
	log.Info().Msg(info)
	_ = logFile.Sync()
	_ = logFile.Close()
}

func ConsoleError(msg string) {
	dir := "./logs"
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal().Err(err).Msg("无法创建日志目录")
	}
	logFileName := fmt.Sprintf("%s/%s%s", dir, time.Now().Format("2006-01-02"), ".log")
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal().Err(err).Msg("无法打开日志文件")
	}
	defer func() {
		_ = logFile.Close()
	}()
	log.Logger = zerolog.New(logFile).With().Timestamp().Logger()
	log.Error().Msg(msg)
}

func (lw *TLogWriter) WriteLocal(info string) {
	// 设置日志文件名格式
	logFileName := time.Now().Format("2006-01-02") + ".log"
	logFilePath := filepath.Join(lw.logDir, logFileName)

	// 打开日志文件
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal().Err(err).Msg("无法打开日志文件")
	}
	defer func() {
		_ = logFile.Close()
	}()
	log.Logger = zerolog.New(logFile).With().Timestamp().Logger()
	log.Error().Msg(info)
}
func (lw *TLogWriter) WriteError(info string, printConsole bool) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if printConsole {
		fmt.Println(info)
	}
	lw.logCtl.LogType = logCtl.ErrorLog
	lw.logCtl.LogInfo = info
	if err := lw.logCtl.WriteLog(info); err != nil {
		lw.WriteLocal(info)
	}
}
func (lw *TLogWriter) WriteInfo(info string, printConsole bool) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if printConsole {
		fmt.Println(info)
	}
	lw.logCtl.LogType = logCtl.InfoLog
	lw.logCtl.LogInfo = info
	if err := lw.logCtl.WriteLog(info); err != nil {
		lw.WriteLocal(info)
	}
}
func (lw *TLogWriter) WriteDebug(info string, printConsole bool) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if printConsole {
		fmt.Println(info)
	}
	lw.logCtl.LogType = logCtl.DebugLog
	lw.logCtl.LogInfo = info
	if err := lw.logCtl.WriteLog(info); err != nil {
		lw.WriteLocal(info)
	}
}
