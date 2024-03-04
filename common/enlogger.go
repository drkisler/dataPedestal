package common

import (
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type TLogInfo struct {
	LogID   int64  `json:"log_id,omitempty"`
	LogDate string `json:"log_date,omitempty"`
	LogTime string `json:"log_time,omitempty"`
	LogInfo string `json:"log_info,omitempty"`
}

type TLogQuery struct {
	LogDate   string `json:"log_date"`
	PageSize  int32  `json:"page_size,omitempty"`
	PageIndex int32  `json:"page_index,omitempty"`
}

var LogServ TLogService

type TLogger struct {
	logger   *log.Logger
	logFile  *os.File
	logDate  string
	filePath string
	fileName string
	lock     *sync.Mutex
}
type TLogService struct {
	infoLogger  *TLogger
	warnLogger  *TLogger
	errorLogger *TLogger
	debugLogger *TLogger
	isDebug     bool
}

func NewLogService(currentPath, pathSeparator, infoPath, warnPath, errorPath, debugPath string, isDebug bool) {
	var encodeFilePath = func(currentPath, pathSeparator, subDir string) string {
		arrDir := strings.Split(currentPath, pathSeparator)
		if arrDir[len(arrDir)-1] == "" {
			arrDir[len(arrDir)-1] = subDir
		} else {
			arrDir = append(arrDir, subDir)
		}
		arrDir = append(arrDir, "")
		result := strings.Join(arrDir, pathSeparator)
		_, err := os.Stat(result)
		if err != nil {
			if os.IsNotExist(err) {
				err = os.Mkdir(result, 0755)
				if err != nil {
					log.Fatal(err)
				}
			} else {
				log.Fatal(err)
			}
		}

		return strings.Join(arrDir, pathSeparator)
	}
	LogServ.infoLogger = newLogger(encodeFilePath(currentPath, pathSeparator, infoPath))
	LogServ.warnLogger = newLogger(encodeFilePath(currentPath, pathSeparator, warnPath))
	LogServ.errorLogger = newLogger(encodeFilePath(currentPath, pathSeparator, errorPath))
	LogServ.debugLogger = newLogger(encodeFilePath(currentPath, pathSeparator, debugPath))
	LogServ.isDebug = isDebug
}

func CloseLogService() {
	LogServ.infoLogger.CloseLog()
	LogServ.warnLogger.CloseLog()
	LogServ.errorLogger.CloseLog()
	LogServ.debugLogger.CloseLog()
}

func (ls *TLogService) Info(v ...any) {
	ls.infoLogger.writeLog(v...)
}
func (ls *TLogService) Warn(v ...any) {
	ls.warnLogger.writeLog(v...)
}
func (ls *TLogService) Error(v ...any) {
	ls.errorLogger.writeLog(v...)
}
func (ls *TLogService) Debug(v ...any) {
	if ls.isDebug {
		ls.debugLogger.writeLog(v...)
	}
}

func newLogger(filePath string) *TLogger {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)
	return &TLogger{logger: logger, filePath: filePath, lock: &sync.Mutex{}}
}
func (enLog *TLogger) CloseLog() {
	enLog.lock.Lock()
	defer enLog.lock.Unlock()
	_ = enLog.logFile.Close()
}
func (enLog *TLogger) newFile() {
	_ = enLog.logFile.Close()
	var err error
	enLog.fileName = enLog.filePath + "log_" + time.Now().Format("20060102") + ".log"
	if enLog.logFile, err = os.OpenFile(enLog.fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766); err != nil {
		log.Fatal(err)
	}
	enLog.logger.SetOutput(enLog.logFile)
	enLog.logDate = time.Now().Format("20060102")
}

func (enLog *TLogger) writeLog(v ...interface{}) {
	enLog.lock.Lock()
	defer enLog.lock.Unlock()
	if enLog.logDate != time.Now().Format("20060102") {
		enLog.newFile()
	}
	enLog.logger.Println(v...)
}
