package license

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
)

// GenerateProductCode mac + uuid + fileHash ---->product key
func GenerateProductCode(pluginUUid, fileHash string) string {
	var mac []string
	mac = append(mac, fileHash)
	mac = append(mac, pluginUUid)
	netInterface, err := net.Interfaces()
	if err == nil {
		for _, ifa := range netInterface {
			address := ifa.HardwareAddr.String()
			if address != "" {
				mac = append(mac, address)
			}
		}
	} else {
		mac = append(mac, GetDefaultKey())
	}

	h := sha256.New()
	h.Write([]byte(strings.Join(mac, ",")))
	data := h.Sum(nil)
	snData := make([]byte, len(data)-24)
	for i := 0; i < len(data)-24; i++ {
		snData[i] = data[i] ^ data[i+8] ^ data[i+16] ^ data[i+24]
	}
	return fmt.Sprintf("%x-%x-%x-%x", snData[0:2], snData[2:4], snData[4:6], snData[6:8])
}

// GenerateLicenseCode productKey SerialNumber -----> licenseCode
func GenerateLicenseCode(pluginUUid, SerialNumber string) string {
	source := []string{pluginUUid, SerialNumber}
	h := sha256.New()
	h.Write([]byte(strings.Join(source, ",")))
	data := h.Sum(nil)
	snData := make([]byte, len(data)-24)
	for i := 0; i < len(data)-24; i++ {
		snData[i] = data[i] ^ data[i+8] ^ data[i+16] ^ data[i+24]
	}
	sn := fmt.Sprintf("%x-%x-%x-%x", snData[0:2], snData[2:4], snData[4:6], snData[6:8])
	return sn
}

// HashString 将32位字符哈希成64为字符，用于授权码
func HashString(input string) string {
	hasher := sha256.New()
	hasher.Write([]byte(input))
	return hex.EncodeToString(hasher.Sum(nil))
}

/*
func VerifyLicense(key, license string) bool {
	sn := GenerateProductCode(key)
	return sn == license
}
*/

func getEnvironmentVariable(key string) (string, error) {
	// 获取用户主目录  /home/用户名/.bashrc
	homeDir := os.ExpandEnv("$HOME")

	// 构造 .bashrc 文件的完整路径
	bashrcPath := filepath.Join(homeDir, ".bashrc")

	// 读取文件内容
	content, err := os.ReadFile(bashrcPath)
	if err != nil {
		return "", fmt.Errorf("读取文件失败: %v", err)
	}

	// 逐行解析文件内容
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	for scanner.Scan() {
		line := scanner.Text()
		// 查找 export 语句
		if strings.HasPrefix(line, "export") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				lineKey := strings.TrimSpace(strings.TrimPrefix(parts[0], "export"))
				if lineKey == key {
					value := strings.TrimSpace(parts[1])
					// 移除可能的引号
					value = strings.Trim(value, "\"'")
					return value, nil
				}
			}
		}
	}

	if err = scanner.Err(); err != nil {
		return "", fmt.Errorf("解析文件时出错: %v", err)
	}

	return "", fmt.Errorf("未找到环境变量: %s", key)
}

func GetDefaultKey() string {
	result := os.Getenv("DEFAULT_KEY")
	if result == "" {
		result, _ = getEnvironmentVariable("DEFAULT_KEY")
		if result == "" {
			result = "Enjoy0rZpJAcL6OnUsORc3XohRpIBUjy"
		}
		_ = os.Setenv("DEFAULT_KEY", result)
	}
	return result
}

func FileHash(filePath string) (string, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("读取文件失败: %v", err)
	}
	harsher := sha256.New()
	harsher.Write(file)
	hashValue := harsher.Sum(nil)
	return hex.EncodeToString(hashValue), nil
}

func DecryptAES(cipherText, key string) (string, error) {
	cipherTextBytes, err := base64.StdEncoding.DecodeString(cipherText)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}
	if len(cipherTextBytes) < aes.BlockSize {
		return "", fmt.Errorf("cipherText too short")
	}
	iv := cipherTextBytes[:aes.BlockSize]
	cipherTextBytes = cipherTextBytes[aes.BlockSize:]
	mode := cipher.NewCFBDecrypter(block, iv)
	mode.XORKeyStream(cipherTextBytes, cipherTextBytes)
	return string(cipherTextBytes), nil
}

func EncryptAES(plainText, key string) (string, error) {
	plainTextBytes := []byte(plainText)
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}
	cipherText := make([]byte, aes.BlockSize+len(plainTextBytes))
	iv := cipherText[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}
	mode := cipher.NewCFBEncrypter(block, iv)
	mode.XORKeyStream(cipherText[aes.BlockSize:], plainTextBytes)
	return base64.StdEncoding.EncodeToString(cipherText), nil
}
