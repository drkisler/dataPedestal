package module

import (
	"github.com/drkisler/dataPedestal/common"
)

type TPluginInfo = common.TPluginInfo
type TPlugin struct {
	TPluginInfo
	LicenseCode string `json:"license_code"`
	ProductCode string `json:"product_code"`
}

// ToByte 将 PluginUUID、PluginFile、RunType、PluginConfig 写入二进制串中,包括长度信息
func (p *TPlugin) ToByte() []byte {
	var result []byte
	//PluginUUID、PluginFile、RunType 长度<256
	appendData := func(source string, withLength bool) {
		data := []byte(source)
		length := len(data)
		if withLength {
			result = append(result, uint8(length))
			result = append(result, data...)
		} else {
			result = append(result, data...)
		}
	}
	appendData(p.PluginUUID, true)
	appendData(p.PluginFile, true)
	appendData(p.RunType, true)
	appendData(p.PluginConfig, false)
	return result
}

// FromByte 从二进制串中提取 PluginUUID、PluginFile、RunType、PluginConfig
func (p *TPlugin) FromByte(source []byte) error {
	var index int
	var length int
	var err error
	//PluginUUID、PluginFile、RunType 长度<256
	getData := func(withLength bool) string {
		if withLength {
			length = int(source[index])
			index++
			data := source[index : index+length]
			index += length
			return string(data)
		}
		data := source[index:]
		return string(data)
	}
	p.PluginUUID = getData(true)
	p.PluginFile = getData(true)
	p.RunType = getData(true)
	p.PluginConfig = getData(false)
	return err
}

func (p *TPlugin) AddPlugin() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	if err = dbs.AddPlugin(p); err != nil {
		return err
	}
	mdb, err := GetMemServ()
	if err != nil {
		return err
	}
	if err = mdb.AddPlugin(p); err != nil {
		return err
	}
	return nil
}

func (p *TPlugin) DelPlugin() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	if err = dbs.DeletePlugin(p.PluginUUID); err != nil {
		return err
	}
	mdb, err := GetMemServ()
	if err != nil {
		return err
	}
	if err = mdb.DeletePlugin(p.PluginUUID); err != nil {
		return err
	}
	return nil
}

func (p *TPlugin) GetPluginList() ([]TPlugin, int, error) {
	dbs, err := GetMemServ()
	if err != nil {
		return nil, -1, err
	}
	data, err := dbs.GetPluginList()
	if err != nil {
		return nil, -1, err
	}
	return data, len(data), nil
}

func (p *TPlugin) AlterRunType() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}

	if err = dbs.AlterPluginRunType(p.PluginUUID, p.RunType); err != nil {
		return err
	}
	mdb, err := GetMemServ()
	if err != nil {
		return err
	}
	if err = mdb.AlterPluginRunType(p.PluginUUID, p.RunType); err != nil {
		return err
	}
	return nil
}

func (p *TPlugin) AlterFile() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	if err = dbs.AlterPluginFile(p.PluginUUID, p.PluginFile); err != nil {
		return err
	}
	mdb, err := GetMemServ()
	if err != nil {
		return err
	}
	if err = mdb.AlterPluginFile(p.PluginUUID, p.PluginFile); err != nil {
		return err
	}
	return nil
}

func (p *TPlugin) AlterPluginLicense() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	if err = dbs.AlterPluginLicense(p.PluginUUID, p.LicenseCode, p.ProductCode); err != nil {
		return err
	}
	mdb, err := GetMemServ()
	if err != nil {
		return err
	}
	if err = mdb.AlterPluginLicense(p.PluginUUID, p.LicenseCode, p.ProductCode); err != nil {
		return err
	}
	return nil
}

func (p *TPlugin) InitByUUID() error {
	dbs, err := GetMemServ()
	if err != nil {
		return err
	}
	return dbs.InitPluginByUUID(p)
}

func (p *TPlugin) AlterConfig() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	if err = dbs.AlterPluginConfig(p.PluginUUID, p.PluginConfig); err != nil {
		return err
	}
	mdb, err := GetMemServ()
	if err != nil {
		return err
	}
	if err = mdb.AlterPluginConfig(p.PluginUUID, p.PluginConfig); err != nil {
		return err
	}
	return nil
}

func GetAutoRunPlugins() ([]TPlugin, error) {
	dbs, err := GetMemServ()
	if err != nil {
		return nil, err
	}
	return dbs.GetAutoRunPlugins()
}

func ClearPlugin() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	if err = dbs.ClearPlugin(); err != nil {
		return err
	}
	mdb, err := GetMemServ()
	if err != nil {
		return err
	}
	if err = mdb.ClearPlugin(); err != nil {
		return err
	}
	return nil
}
