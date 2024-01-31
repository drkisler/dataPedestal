package initializers

type TConfigure struct {
	IsDebug      bool   `json:"is_debug"`
	SerialNumber string `json:"serial_number"`
	LicenseCode  string `json:"license_code"`
}
