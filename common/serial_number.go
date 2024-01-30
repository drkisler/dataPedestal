package common

import (
	"crypto/sha256"
	"fmt"
	"net"
	"strings"
)

func GenerateCaptcha(key string) (string, error) {
	netInterface, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	var mac []string
	for _, ifa := range netInterface {
		address := ifa.HardwareAddr.String()
		if address != "" {
			mac = append(mac, address)
		}
	}
	h := sha256.New()
	h.Write([]byte(strings.Join(mac, ",")))
	data := h.Sum([]byte(key))
	snData := make([]byte, 16)
	for i := 0; i < 16; i++ {
		snData[i] = data[i] ^ data[i+16]
	}
	sn := fmt.Sprintf("%x-%x-%x-%x-%x", snData[0:4], snData[4:6], snData[6:8], snData[8:10], snData[10:])
	return sn, nil
}
func VerifyCaptcha(key, captcha string) (bool, error) {
	sn, err := GenerateCaptcha(key)
	if err != nil {
		return false, err
	}
	return sn == captcha, nil
}
