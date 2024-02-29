package fileService

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/drkisler/utils"
	"io"
	"net"
	"os"
	"sync"
)

type TFileMeta struct {
	FileName     string `json:"file_name"`
	FileSize     int64  `json:"file_size"`
	FileUUID     string `json:"file_uuid"` // pluginUUID
	FileConfig   string `json:"file_config"`
	SerialNumber string `json:"serial_number"`
	RunType      string `json:"run_type"`
}
type FHandleFileMeta func(meta *TFileMeta, err error)

func (fm *TFileMeta) CheckValid() bool {
	return fm.FileName != "" && fm.FileSize > 0 && fm.FileUUID != "" && fm.FileConfig != "" && fm.SerialNumber != "" && fm.RunType != ""
}

type TFileService struct {
	port     int32
	metaFunc FHandleFileMeta
	wg       sync.WaitGroup
	stopChan chan int32
	listener net.Listener
}

// NewFileService 创建文件服务
func NewFileService(port int32, metaFunc FHandleFileMeta) (*TFileService, error) {
	li, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	return &TFileService{
		port:     port,
		stopChan: make(chan int32, 1),
		metaFunc: metaFunc,
		wg:       sync.WaitGroup{},
		listener: li,
	}, nil
}

func (fs *TFileService) Start() {
	fs.wg.Add(1)
	go fs.run(&fs.wg, &fs.stopChan, fs.metaFunc)

}

// Stop 停止服务
func (fs *TFileService) Stop() {
	fs.stopChan <- 1
	fs.wg.Wait()
}

func (fs *TFileService) run(wg *sync.WaitGroup, stopChan *chan int32, metaFunc FHandleFileMeta) {
	defer wg.Done()
	var connWg sync.WaitGroup
	for {
		select {
		case <-*stopChan:
			_ = fs.listener.Close()
			connWg.Wait()
		default:
			conn, err := fs.listener.Accept()
			if err != nil {
				break
			}
			connWg.Add(1)
			go func(conn net.Conn, wg *sync.WaitGroup) {
				defer func() {
					_ = conn.Close()
					wg.Done()
				}()
				metaFunc(ReceiveFile(conn))
			}(conn, &connWg)
		}
	}

}

func ReceiveFile(conn net.Conn) (*TFileMeta, error) {
	var err error
	sizeBuff := make([]byte, 4)
	if _, err = conn.Read(sizeBuff); err != nil {
		return nil, err
	}
	metaSize := binary.LittleEndian.Uint32(sizeBuff)
	metaBuff := make([]byte, metaSize)
	if _, err = conn.Read(metaBuff); err != nil {
		return nil, err
	}
	var fileMeta TFileMeta
	if err = json.Unmarshal(metaBuff, &fileMeta); err != nil {
		return nil, err
	}
	if !fileMeta.CheckValid() {
		return nil, fmt.Errorf("%v format error", fileMeta)
	}
	//创建FileUUID目录
	fp, err := utils.NewFilePath()
	if err != nil {
		return nil, err
	}
	fileMap := make(map[string]string)
	fileMap["filePath"] = fileMeta.FileUUID
	if err = fp.SetFileDir(&fileMap); err != nil {
		return nil, err
	}
	fileFullName := fileMap["filePath"] + fileMeta.FileName
	// 如果fileFullName文件已经存在则删除，
	if _, err = os.Stat(fileFullName); err == nil {
		if err = os.Remove(fileFullName); err != nil {
			return nil, err
		}
	}
	// 创建文件fileFullName
	file, err := os.Create(fileFullName)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()
	if _, err = io.CopyN(file, conn, fileMeta.FileSize); err != nil {
		return nil, err
	}

	return &fileMeta, nil
}
