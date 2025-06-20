package main

import (
	"github.com/drkisler/dataPedestal/universal/databaseDriver/driverwrapper"
)

// 用于将数据库将数据库驱动封装成C语言的动态库，解决GO语言插件版本不一致的问题
// go build -buildmode=c-shared -o libs/mysqlDriver.so mysql_driver.go mysql.go

func init() {
	// 注册 MySQL 驱动
	driverwrapper.RegisterDriver(NewMySQLDriver)
}

func main() {}
