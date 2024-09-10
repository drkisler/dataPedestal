package common

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"os"
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

func GetDefaultKey() string {
	return "Enjoy0rZpJAcL6OnUsORc3XohRpIBUjy"
}

func FileHash(filePath string) (string, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	hasher := sha256.New()
	hasher.Write(file)
	hashValue := hasher.Sum(nil)
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
