package main

import (
	"fmt"
	"github.com/drkisler/dataPedestal/universal/databaseDriver"
	"plugin"
)

func main() {
	// 加载插件
	pluginFile := "mysql.so"
	p, err := plugin.Open(pluginFile)
	if err != nil {
		fmt.Println(fmt.Printf("open plugin file : %s", err.Error()))
		return
	}
	var driverSymbol plugin.Symbol
	if driverSymbol, err = p.Lookup("NewDbDriver"); err != nil {
		fmt.Println(fmt.Printf("lookup symbol : %s", err.Error()))
		return
	}
	newDbDriver, ok := driverSymbol.(func(fileName string) (databaseDriver.IDbDriver, error))
	if !ok {
		fmt.Println(fmt.Printf("无效的 NewDbDriver 函数类型!"))
		return
	}
	driver, err := newDbDriver(pluginFile) //newDbDriver(`{"host":"192.168.110.130:3306","dbname":"sanyu","user":"sanyu","password":"sanyu"}`, 2, 5, 15, 5)
	if err != nil {
		fmt.Println(fmt.Printf("create driver error :%s", err.Error()))
		return
	}
	//fmt.Println(driver.GetSchema())

	if err = driver.OpenConnect(`{"host":"192.168.110.130:3306","dbname":"sanyu","user":"sanyu","password":"sanyu"}`, 2, 5, 15, 5); err != nil {
		fmt.Println(fmt.Printf("open connect error :%s", err.Error()))
		return
	}

	if err = driver.CloseConnect(); err != nil {
		fmt.Println(fmt.Printf("close driver error :%s", err.Error()))
		return
	}

	newDriver, err := driver.NewConnect(`{"host":"192.168.110.130:3306","dbname":"sanyu","user":"sanyu","password":"sanyu"}`, 2, 5, 15, 5)
	if err != nil {
		fmt.Println(fmt.Printf("create driver error :%s", err.Error()))
		return
	}
	fmt.Println(newDriver.GetSchema())
	if err = newDriver.CloseConnect(); err != nil {
		fmt.Println(fmt.Printf("close driver error :%s", err.Error()))
		return
	}

	fmt.Println("OK!")
	/*



		openConnectSym, err := p.Lookup("Connect")
		if err != nil {
			fmt.Println(fmt.Printf("lookup connect :%s", err.Error()))
			return
		}
		openConnectFunc, ok := openConnectSym.(func(connectStr string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) error)
		if !ok {
			fmt.Println("无效的connect函数类型!")
			return
		}

		closeConnect, err := p.Lookup("Close")
		if err != nil {
			fmt.Println(fmt.Printf("lookup close :%s", err.Error()))
			return
		}
		closeFunc, ok := closeConnect.(func() error)
		if !ok {
			fmt.Println("无效的close函数类型!")
			return
		}

		GetSchemaSym, err := p.Lookup("GetSchema")
		if err != nil {
			fmt.Println(fmt.Printf("lookup close :%s", err.Error()))
			return
		}
		GetSchemaFunc, ok := GetSchemaSym.(func() string)
		if !ok {
			fmt.Println("无效的GetSchema函数类型!")
			return
		}

		if err = openConnectFunc(`{"host":"192.168.110.130:3306","dbname":"sanyu","user":"sanyu","password":"sanyu"}`, 2, 5, 15, 5); err != nil {
			fmt.Println(fmt.Printf("call function Connect error :%s", err.Error()))
			return
		}

		fmt.Println(GetSchemaFunc())

		if err = closeFunc(); err != nil {
			fmt.Println(fmt.Printf("call function Close error :%s", err.Error()))
			return
		}
		fmt.Println("OK!")

	*/
}
