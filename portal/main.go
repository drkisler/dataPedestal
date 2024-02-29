package main

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/portal/control"
	"github.com/drkisler/dataPedestal/portal/module"
	"github.com/drkisler/dataPedestal/portal/service"
	"github.com/drkisler/dataPedestal/universal/messager"
	usrServ "github.com/drkisler/dataPedestal/universal/userAdmin/service"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"github.com/takama/daemon"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"time"
)

const (
	managerName = "pluginService"
	serverDesc  = "插件服务门户"
	usageHelp   = "Usage: pluginService install | remove | start | stop | status"
)

type TManagerDaemon struct {
	daemon.Daemon
}

func createAndStartGinService() {
	gin.SetMode(gin.ReleaseMode)
	//启动服务
	r := gin.Default()
	r.MaxMultipartMemory = 8 << 20

	r.Any("/api/:uuid", reverseProxy("http://localhost:8080"))

	r.POST("/login", usrServ.Login)
	r.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"code": -1, "message": "api not found"})
	})

	user := r.Group("/user")
	user.Use(common.SetHeader, utils.AuthMiddleware)
	user.POST("/delete", usrServ.DeleteUser)
	user.POST("/add", usrServ.AddUser)
	user.POST("/alter", usrServ.AlterUser)
	user.POST("/get", usrServ.QueryUser)
	user.POST("/reset", usrServ.ResetPassword)
	user.POST("/checkUser", usrServ.CheckUser)
	plugin := r.Group("/plugin")
	plugin.Use(common.SetHeader, utils.AuthMiddleware)
	plugin.POST("/delete", service.DeletePlugin)                 //删除插件
	plugin.POST("/add", service.AddPlugin)                       // 新增插件
	plugin.POST("/alter", service.AlterPlugin)                   //修改插件
	plugin.POST("/get", service.QueryPlugin)                     //获取插件列表，含插件运行状态
	plugin.POST("/setRunType", service.SetRunType)               //设置运行放松
	plugin.POST("/upload", service.Upload)                       //上传插件
	plugin.GET("/download", service.Download)                    //下载插件
	plugin.POST("/updateConfig", service.UpdateConfig)           //修改配置
	plugin.POST("/loadPlugin", service.LoadPlugin)               //加载插件
	plugin.POST("/unloadPlugin", service.UnloadPlugin)           //卸载插件
	plugin.POST("/runPlugin", service.RunPlugin)                 //运行插件
	plugin.POST("/stopPlugin", service.StopPlugin)               //停止插件
	plugin.POST("/getTempConfig", service.GetTempConfig)         //获取模板
	plugin.POST("/getPluginNameList", service.GetPluginNameList) //获取加载后的插件列表
	plugin.POST("/pubPlugin/:hostUUID", service.PubPlugin)
	plugin.GET("/getHosts", service.GetHosts)
	logs := r.Group("/logger")
	logs.Use(common.SetHeader, utils.AuthMiddleware)
	logs.POST("/getLogDate", service.GetLogDate)
	logs.POST("/getLogInfo", service.GetLogInfo)
	logs.POST("/delOldLog", service.DelOldLog)
	logs.POST("/delLog", service.DelLog)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", initializers.PortalCfg.ServicePort),
		Handler: r,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			_ = utils.LogServ.WriteLog(common.ERROR_PATH, fmt.Sprintf("listen: %s\n", err.Error()))

		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		_ = utils.LogServ.WriteLog(common.ERROR_PATH, "Server Shutdown:", err)
	}
	// 停止服务

	//<-interrupt
	//停止服务
	_ = utils.LogServ.WriteLog(common.INFO_PATH, "Server Shutdown")
}
func reverseProxy(target string) gin.HandlerFunc {
	remoteUrl, _ := url.Parse(target)
	proxy := httputil.NewSingleHostReverseProxy(remoteUrl)
	return func(c *gin.Context) {
		c.Request.URL.Host = remoteUrl.Host
		c.Request.URL.Scheme = remoteUrl.Scheme
		c.Request.Header.Set("X-Forwarded-Host", c.Request.Header.Get("Host"))
		c.Request.Host = remoteUrl.Host
		proxy.ServeHTTP(c.Writer, c.Request)
	}
}
func (serv *TManagerDaemon) Manage() (string, error) {
	if len(os.Args) > 1 {
		command := os.Args[1]
		switch command {
		case "install":
			return serv.Install()
		case "remove":
			return serv.Remove()
		case "start":
			return serv.Start()
		case "stop":
			return serv.Stop()
		case "status":
			return serv.Status()
		default:
			return usageHelp, nil
		}
	}

	createAndStartGinService()

	return managerName + " exited", nil
}
func main() {
	var err error

	// region 读取配置文件
	files, err := utils.NewFilePath()
	if err != nil {
		fmt.Printf("设置日志目录失败：%s", err.Error())
		os.Exit(1)
	}
	if err = initializers.PortalCfg.LoadConfig(files); err != nil {
		fmt.Printf("读取配置文件失败：%s", err.Error())
		os.Exit(1)
	}
	if err = initializers.PortalCfg.InitConfig(); err != nil {
		fmt.Printf("初始化配置文件失败：%s", err.Error())
		os.Exit(1)
	}
	/*	if err = initializers.PortalCfg.InitLogfile(); err != nil {
		fmt.Printf("初始化日志文件失败：%s", err.Error())
		os.Exit(1)
	}*/
	// endregion

	// region 初始化数据库
	module.DbFilePath = (*files.FileDirs)[common.DATABASE_TATH]
	dbs, err := module.GetDbServ()
	if err != nil {
		fmt.Printf("初始化数据库失败：%s", err.Error())
		os.Exit(1)
	}
	defer func() {
		_ = dbs.CloseDB()
	}()

	if err = usrServ.ConnectToDB((*files.FileDirs)[common.DATABASE_TATH]); err != nil {
		fmt.Printf("初始化user数据库失败：%s", err.Error())
		os.Exit(1)
	}
	defer func() {
		_ = usrServ.CloseConnect()
	}()
	// endregion

	// region 创建并启动心跳检测服务
	control.Survey, err = messager.NewVote(initializers.PortalCfg.SurveyUrl)
	if err != nil {
		fmt.Printf("创建心跳检测服务失败：%s", err.Error())
		os.Exit(1)
	}
	control.Survey.Run()
	defer control.Survey.Stop()
	// endregion

	// region 创建并对话client
	control.MsgClient, err = messager.NewMessageClient()
	if err != nil {
		fmt.Printf("创建消息服务失败：%s", err.Error())
		os.Exit(1)
	}

	defer control.MsgClient.Close()
	// endregion

	// region 创建并启动daemon
	srv, err := daemon.New(managerName, serverDesc, daemon.SystemDaemon)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	servDaemon := &TManagerDaemon{srv}
	status, err := servDaemon.Manage()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	// endregion
	fmt.Println(status)
}
