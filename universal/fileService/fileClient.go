package fileService

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"syscall"
)

func SendFile(fileServUrl, fileUUID, config, runType, serialNumber string, file *os.File) error {
	conn, err := net.Dial("tcp", fileServUrl)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	fileMeta := TFileMeta{
		FileName:     fileInfo.Name(),
		FileSize:     fileInfo.Size(),
		FileUUID:     fileUUID,
		FileConfig:   config,
		RunType:      runType,
		SerialNumber: serialNumber,
	}
	metaData, err := json.Marshal(&fileMeta)
	if err != nil {
		return err
	}
	sizeBuff := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuff, uint32(len(metaData)))
	if _, err = conn.Write(sizeBuff); err != nil {
		return err
	}
	if _, err = conn.Write(metaData); err != nil {
		return err
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("conn error ")
	}
	tcpFile, err := tcpConn.File()
	if err != nil {
		return err
	}
	if _, err = syscall.Sendfile(int(tcpFile.Fd()), int(file.Fd()), nil, int(fileInfo.Size())); err != nil {
		return err
	}
	return nil
}
