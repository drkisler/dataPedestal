package fileService

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/genService"
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
	port       int32
	metaFunc   FHandleFileMeta
	wg         sync.WaitGroup
	stopChan   chan int32
	listener   net.Listener
	fileFolder string
	isRunning  bool
}

// NewFileService 创建文件服务
func NewFileService(port int32, folder string, metaFunc FHandleFileMeta) (*TFileService, error) {
	li, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}

	return &TFileService{
		port:       port,
		stopChan:   make(chan int32, 1),
		metaFunc:   metaFunc,
		wg:         sync.WaitGroup{},
		fileFolder: folder,
		listener:   li,
	}, nil
}

func (fs *TFileService) Start() {
	fs.wg.Add(1)
	fs.isRunning = true
	go fs.run()
}

// Stop 停止服务
func (fs *TFileService) Stop() {
	_ = fs.listener.Close()
	fs.isRunning = false
	fs.wg.Wait()
}

func (fs *TFileService) run() {
	defer fs.wg.Done()
	var connWg sync.WaitGroup
	for fs.isRunning {
		conn, err := fs.listener.Accept()
		if err != nil {
			//屏蔽下面的异步处理
			continue
		}
		connWg.Add(1)
		go func(conn net.Conn, wg *sync.WaitGroup) {
			defer func() {
				_ = conn.Close()
				wg.Done()
			}()
			fs.metaFunc(ReceiveFile(conn, fs.fileFolder))
		}(conn, &connWg)
	}

}

func ReceiveFile(conn net.Conn, folder string) (*TFileMeta, error) {
	var err error
	var fileMeta TFileMeta
	sizeBuff := make([]byte, 4)
	if _, err = conn.Read(sizeBuff); err != nil {
		fileMeta.FileSize = -1
		fileMeta.FileName = "conn.Read [4]byte"
		return &fileMeta, err
	}
	metaSize := binary.LittleEndian.Uint32(sizeBuff)
	metaBuff := make([]byte, metaSize)
	if _, err = conn.Read(metaBuff); err != nil {
		fileMeta.FileSize = -1
		fileMeta.FileName = fmt.Sprintf("conn.Read [%d]byte", metaSize)
		return &fileMeta, err
	}
	if err = json.Unmarshal(metaBuff, &fileMeta); err != nil {
		fileMeta.FileSize = -1
		fileMeta.FileName = fmt.Sprintf("fileMeta %s", string(metaBuff))
		return &fileMeta, err
	}
	if !fileMeta.CheckValid() {
		fileMeta.FileSize = -1
		fileMeta.FileName = fmt.Sprintf("fileMeta.CheckValid() %s", string(metaBuff))
		return &fileMeta, fmt.Errorf("%v format error", fileMeta)
	}
	// check the file folder exist or not and create it if not exist
	fileDir := genService.GenFilePath(folder, fileMeta.FileUUID)
	if _, err = os.Stat(fileDir); err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(fileDir, 0766)
			if err != nil {
				return nil, fmt.Errorf("创建目录%s出错:%s", fileDir, err.Error())
			}
		}
	}
	// check the file exist or not and remove it if exist
	filePath := genService.GenFilePath(folder, fileMeta.FileUUID, fileMeta.FileName)
	if _, err = os.Stat(filePath); err == nil {
		if err = os.Remove(filePath); err != nil {
			return nil, fmt.Errorf("删除旧文件%s失败:%s", filePath, err.Error())
		}
	}
	// 创建文件fileFullName
	file, err := os.Create(filePath)
	if err != nil {
		fileMeta.FileSize = -1
		return &fileMeta, err
	}
	defer func() {
		_ = file.Close()
	}()
	if _, err = io.CopyN(file, conn, fileMeta.FileSize); err != nil {
		fileMeta.FileSize = -1
		return &fileMeta, err
	}

	if err = os.Chmod(filePath, 0755); err != nil {
		fileMeta.FileSize = -1
		return &fileMeta, err
	}

	return &fileMeta, nil
	/*


		//创建FileUUID目录
		currentPath, err := os.Executable()
		if err != nil {
			fileMeta.FileSize = -1
			fileMeta.FileName = "os.Executable()"
			return &fileMeta, err
		}
		dirFlag := string(os.PathSeparator)

		arrPath := strings.Split(currentPath, dirFlag)
		arrPath = arrPath[:len(arrPath)-1]
		arrPath = append(arrPath, folder)


		parentFolder := strings.Join(arrPath, dirFlag)
		if _, err = os.Stat(parentFolder); err != nil {
			if os.IsNotExist(err) {
				err = os.Mkdir(parentFolder, 0766)
				if err != nil {
					fileMeta.FileSize = -1
					return &fileMeta, fmt.Errorf("创建目录%s出错:%s", parentFolder, err.Error())
				}
			}
		}
		arrPath = append(arrPath, fileMeta.FileUUID)
		pluginFolder := strings.Join(arrPath, dirFlag)
		if _, err = os.Stat(pluginFolder); err != nil {
			if os.IsNotExist(err) {
				err = os.Mkdir(pluginFolder, 0766) //0766
				if err != nil {
					fileMeta.FileSize = -1
					return &fileMeta, fmt.Errorf("创建目录%s出错:%s", pluginFolder, err.Error())
				}
			}
		}
		arrPath = append(arrPath, fileMeta.FileName)
		fileFullName := strings.Join(arrPath, dirFlag)
		// 如果fileFullName文件已经存在则删除，
		if _, err = os.Stat(fileFullName); err == nil {
			if err = os.Remove(fileFullName); err != nil {
				fileMeta.FileSize = -1
				return &fileMeta, err
			}
		}
		// 创建文件fileFullName
		file, err := os.Create(fileFullName)
		if err != nil {
			fileMeta.FileSize = -1
			return &fileMeta, err
		}
		defer func() {
			_ = file.Close()
		}()
		if _, err = io.CopyN(file, conn, fileMeta.FileSize); err != nil {
			fileMeta.FileSize = -1
			return &fileMeta, err
		}

		if err = os.Chmod(fileFullName, 0755); err != nil {
			fileMeta.FileSize = -1
			return &fileMeta, err
		}

		return &fileMeta, nil
	*/
}
