package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/afocus/captcha"
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/portal/control"
	"github.com/drkisler/dataPedestal/portal/service"
	dsServ "github.com/drkisler/dataPedestal/universal/dataSource/service"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/drkisler/dataPedestal/universal/messager"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	usrServ "github.com/drkisler/dataPedestal/universal/userAdmin/service"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"github.com/takama/daemon"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"
)

const (
	managerName = "pluginService"
	serverDesc  = "插件服务门户"
	usageHelp   = "Usage: pluginService install | remove | start | stop | status"
)

// TManagerDaemon 用于管理插件服务的守护进程
type TManagerDaemon struct {
	daemon.Daemon
}

// TCaptcha 用于存储验证码信息
type TCaptcha struct {
	Code      string    // 验证码内容
	CreatedAt time.Time // 创建时间
	ExpiresIn int       // 有效期（秒）
}

// TRateLimit 用于存储请求频率信息
type TRateLimit struct {
	Count     int       // 请求次数
	LastReset time.Time // 上次重置时间
}

// 存储验证码和频率限制数据
var (
	captchaStore   = make(map[string]TCaptcha)
	rateLimitStore = make(map[string]TRateLimit)
	storeMutex     sync.Mutex
)

// 频率限制中间件
func rateLimitMiddleware() gin.HandlerFunc {
	const maxRequests = 10
	const windowSize = time.Minute

	return func(c *gin.Context) {
		ip := c.ClientIP()
		now := time.Now()

		storeMutex.Lock()

		rl, exists := rateLimitStore[ip]
		if !exists || now.Sub(rl.LastReset) >= windowSize {
			rl = TRateLimit{Count: 0, LastReset: now}
		}

		if rl.Count >= maxRequests {
			storeMutex.Unlock()
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "请求过于频繁，请稍后再试"})
			c.Abort()
			return
		}

		rl.Count++
		rateLimitStore[ip] = rl
		storeMutex.Unlock()
		c.Next()
	}
}

// 生成验证码并返回 Base64
func generateCaptcha(c *gin.Context) {
	capt := captcha.New()
	capt.SetSize(80, 30)
	_ = os.MkdirAll(filepath.Join(os.Getenv("FilePath"), "fonts"), 0755)
	/*
		if err := os.MkdirAll(filepath.Join(os.Getenv("FilePath"), "fonts"), 0755); err != nil {
			fmt.Println(err.Error())
		}
	*/
	err := capt.SetFont(filepath.Join(os.Getenv("FilePath"), "fonts", initializers.PortalCfg.ImageFont)) // 可选字体设置 imageCaptchaFont.ttf Comic.ttf
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "设置字体失败:" + err.Error()})
		return
	}
	capt.SetDisturbance(captcha.HIGH) // 可选干扰设置
	// 设置背景色为淡蓝色
	capt.SetBkgColor(color.RGBA{173, 216, 230, 255})

	img, code := capt.Create(4, captcha.NUM)

	captchaID := fmt.Sprintf("captcha_%d", time.Now().UnixNano())

	storeMutex.Lock()
	captchaStore[captchaID] = TCaptcha{
		Code:      code,
		CreatedAt: time.Now(),
		ExpiresIn: 300,
	}
	storeMutex.Unlock()

	// 将图片编码为 PNG 并转为 Base64
	var buf bytes.Buffer
	if err = png.Encode(&buf, img); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "生成验证码失败"})
		return
	}
	base64Img := base64.StdEncoding.EncodeToString(buf.Bytes())

	// 返回 JSON 响应
	c.JSON(http.StatusOK, struct {
		Code      string `json:"code"`
		CaptchaID string `json:"captcha_id"`
		Image     string `json:"image"`
	}{
		Code:      "0",
		CaptchaID: captchaID,
		Image:     "data:image/png;base64," + base64Img, // 前端可直接使用
	})
}

// 验证码验证中间件
func verifyCaptchaMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		captchaID := c.Query("captcha_id")
		userInput := c.Query("code")

		storeMutex.Lock()
		capt, exists := captchaStore[captchaID]
		storeMutex.Unlock()

		if !exists {
			c.Set("verify_result", "error")
			c.Set("verify_message", "验证码不存在")
			c.Next()
			return
		}

		if time.Since(capt.CreatedAt).Seconds() > float64(capt.ExpiresIn) {
			storeMutex.Lock()
			delete(captchaStore, captchaID)
			storeMutex.Unlock()
			c.Set("verify_result", "error")
			c.Set("verify_message", "验证码已过期")
			c.Next()
			return
		}

		if capt.Code == userInput {
			storeMutex.Lock()
			delete(captchaStore, captchaID)
			storeMutex.Unlock()
			c.Set("verify_result", "success")
			c.Set("verify_message", "验证码正确")
		} else {
			c.Set("verify_result", "error")
			c.Set("verify_message", "验证码错误")
		}

		c.Next()
	}
}

func createAndStartGinService() {
	gin.SetMode(gin.ReleaseMode)
	//启动服务
	r := gin.Default()
	r.MaxMultipartMemory = 8 << 20

	r.GET("/generate-captcha", rateLimitMiddleware(), generateCaptcha)
	r.POST("/login", verifyCaptchaMiddleware(), usrServ.Login)
	r.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"code": -1, "message": "portal api not found:" + c.Request.URL.Path})
	})

	user := r.Group("/user")
	user.Use(genService.SetHeader, utils.AuthMiddleware, Logger())
	user.POST("/delete", usrServ.DeleteUser)
	user.POST("/add", usrServ.AddUser)
	user.POST("/alter", usrServ.AlterUser)
	user.POST("/get", usrServ.QueryUser)
	user.POST("/reset", usrServ.ResetPassword)
	user.POST("/checkUser", usrServ.CheckUser)
	plugin := r.Group("/plugin")
	plugin.Use(genService.SetHeader, utils.AuthMiddleware, Logger())
	plugin.POST("/delete", service.DeletePlugin)       // 删除插件
	plugin.POST("/add", service.AddPlugin)             // 新增插件
	plugin.POST("/alter", service.AlterPlugin)         // 修改插件
	plugin.POST("/get", service.QueryPlugin)           // 获取插件列表，含插件运行状态
	plugin.POST("/setRunType", service.SetRunType)     // 设置运行方式
	plugin.POST("/upload", service.Upload)             // 上传插件
	plugin.GET("/download", service.Download)          // 下载插件
	plugin.POST("/updateConfig", service.UpdateConfig) // 修改配置
	//plugin.POST("/loadPlugin", service.LoadPlugin)                             // 加载插件
	//plugin.POST("/unloadPlugin", service.UnloadPlugin)                         // 卸载插件
	plugin.POST("/runPlugin", service.RunPlugin)                               // 运行插件
	plugin.POST("/stopPlugin", service.StopPlugin)                             // 停止插件
	plugin.POST("/getTempConfig", service.GetTempConfig)                       // 获取模板
	plugin.POST("/getPluginNameList", service.GetPluginNameList)               // 获取加载后的插件列表
	plugin.POST("/pubPlugin/:hostUUID", service.PubPlugin)                     // 部署插件
	plugin.GET("/getHosts", service.GetHosts)                                  // 获取主机清单
	plugin.POST("/takeDown", service.TakeDown)                                 // 取消部署
	plugin.POST("/getProductKey", service.GetProductKey)                       // 获取并验证产品序列号
	plugin.POST("/setLicenseCode/:productSN/:license", service.SetLicenseCode) // 设置授权码
	plugin.POST("/updatePluginFile", service.UpdatePluginFile)                 // 更新插件文件

	dataSource := r.Group("/dataSource")
	dataSource.Use(genService.SetHeader, utils.AuthMiddleware, Logger())
	dataSource.POST("/deleteDataSource", dsServ.DeleteDataSource)             // 删除数据源
	dataSource.POST("/addDataSource", dsServ.AddDataSource)                   // 新增数据源
	dataSource.POST("/updateDataSource", dsServ.UpdateDataSource)             // 修改数据源
	dataSource.POST("/queryDataSource", dsServ.QueryDataSource)               // 获取数据源列表
	dataSource.GET("/getDataSourceNames", dsServ.GetDataSourceNames)          // 获取数据源名称列表
	dataSource.POST("/getDataBaseDrivers", dsServ.GetDataBaseDrivers)         // 获取数据库驱动列表
	dataSource.POST("/getConnectStringByName", dsServ.GetConnectStringByName) // 获取连接字符串
	dataSource.POST("/getOptionsByDriverName", dsServ.GetOptionsByDriverName) // 获取数据库驱动连接选项
	dataSource.POST("/checkConnectString", dsServ.CheckConnectString)         // 测试连接

	logs := r.Group("/logger")
	logs.Use(genService.SetHeader, utils.AuthMiddleware)
	logs.POST("/getPortalLogs", service.GetLogs)
	logs.POST("/deletePortalLogs", service.DeleteLogs)
	logs.POST("/clearPortalLogs", service.ClearLogs)

	logs.POST("/getSysLogDate", service.GetLogDate)
	logs.POST("/getSysLogLocate", service.GetLogLocate)
	logs.GET("/getSysLogInfo/:logTypes/:logDate/:logLocate", service.GetLogInfo)
	logs.POST("/delSysLog", service.DelLog)
	logs.POST("/delSysOldLog", service.DelOldLog)
	logs.POST("/delSysLogByDate", service.DelLogByDate)

	//r.Any("/plugins/:uuid/:route/:api", service.GetTargetUrl)
	plugins := r.Group("/plugins") //使用路由转发的方式
	plugins.Use(genService.SetHeader, utils.AuthMiddleware, Logger())
	plugins.Any("/:uuid/:api", service.PluginAPI)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", initializers.PortalCfg.ServicePort),
		Handler: r,
	}
	go func() {
		_ = srv.ListenAndServe()
	}()
	logService.LogWriter.WriteInfo(fmt.Sprintf("portal服务启动成功,监听端口：%d", initializers.PortalCfg.ServicePort), true)
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)

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
	//str, _ := license.EncryptAES("1P@ssw0rd!", license.GetDefaultKey())
	//fmt.Println(str)
	//str, _ = license.DecryptAES("64DnJBov/BOAjgtKOsCkp94m/p5Z/zL1Iw==", license.GetDefaultKey())
	//fmt.Println(str)
	logService.LogWriter = logService.NewLogWriter("portal")
	file, err := os.Executable()
	if err != nil {
		fmt.Printf("获取可执行文件路径失败：%s", err.Error())
		os.Exit(1)
	}
	_ = os.Setenv("FilePath", filepath.Dir(file))
	_ = os.Setenv("Separator", string(filepath.Separator))

	// region 读取配置文件连接数据库
	if err = initializers.PortalCfg.LoadConfig(genService.GenFilePath("config"), "config.toml"); err != nil {
		fmt.Println(err.Error())
		logService.LogWriter.WriteLocal(fmt.Sprintf("加载配置文件失败：%s", err.Error()))
		os.Exit(1)
	}

	for _, strDir := range []string{"fonts", initializers.PortalCfg.PluginDir, initializers.PortalCfg.DbDriverDir} {
		if err = os.MkdirAll(filepath.Join(os.Getenv("FilePath"), strDir), 0755); err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}

	connectStr, err := initializers.PortalCfg.GetConnection(&initializers.PortalCfg)
	if err != nil {
		fmt.Println(err.Error())
		logService.LogWriter.WriteLocal(fmt.Sprintf("获取数据库连接信息失败：%s", err.Error()))
		os.Exit(1)
	}
	metaDataBase.SetConnectOption(connectStr)
	if _, err = metaDataBase.GetPgServ(); err != nil {
		fmt.Println(err.Error())
		logService.LogWriter.WriteLocal(fmt.Sprintf("连接数据库失败：%s", err.Error()))
		os.Exit(1)
	}
	// endregion

	usrServ.IsDebug = initializers.PortalCfg.IsDebug
	service.IsDebug = initializers.PortalCfg.IsDebug

	// region 创建并启动心跳检测服务,rep
	msg, err := messager.NewMessageServer([]string{initializers.PortalCfg.SurveyUrl},
		control.Survey.HandleOperate)
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("创建心跳检测服务失败：%s", err.Error()), true)
		os.Exit(1)
	}
	msg.Start()
	defer msg.Stop()

	// endregion

	// region 创建并对话client
	control.MsgClient, err = messager.NewMessageClient()
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("创建对话客户端失败：%s", err.Error()), true)
		os.Exit(1)
	}
	defer control.MsgClient.Close()
	// endregion
	// region 创建并启动daemon
	srv, err := daemon.New(managerName, serverDesc, daemon.SystemDaemon)
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("创建daemon失败：%s", err.Error()), true)
		os.Exit(1)
	}
	servDaemon := &TManagerDaemon{srv}
	status, err := servDaemon.Manage()
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("管理daemon失败：%s", err.Error()), true)
		os.Exit(1)
	}
	// endregion
	logService.LogWriter.WriteInfo(fmt.Sprintf("portal服务退出：%s", status), true)
}

func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 开始时间
		startTime := time.Now()

		// 读取请求体
		var requestBody []byte
		if c.Request.Body != nil {
			requestBody, _ = io.ReadAll(c.Request.Body)
		}

		// 将请求体放回，以便后续处理器使用
		c.Request.Body = io.NopCloser(bytes.NewBuffer(requestBody))

		// 创建一个 responseBodyWriter 来捕获响应体
		responseBody := &responseBodyWriter{body: bytes.NewBufferString(""), ResponseWriter: c.Writer}
		c.Writer = responseBody

		// 处理请求
		c.Next()

		// 结束时间
		endTime := time.Now()

		// 执行时间
		latencyTime := fmt.Sprintf("%v", endTime.Sub(startTime))

		// 请求方式
		reqMethod := c.Request.Method

		// 请求路由
		reqUri := c.Request.RequestURI

		// 状态码
		statusCode := fmt.Sprintf("%v", responseBody.Status())

		// 请求IP
		clientIP := c.ClientIP()

		// 获取响应体
		responseJSON := responseBody.body.String()
		if len(responseJSON) == 0 {
			responseJSON = "Empty response"
		} else {
			if !json.Valid([]byte(responseJSON)) {
				responseJSON = "Invalid JSON"
			}
		}

		// 获取请求体
		var requestJSON string
		if len(requestBody) > 0 {
			if json.Valid(requestBody) {
				requestJSON = string(requestBody)
			} else {
				requestJSON = "Invalid JSON"
			}
		} else {
			requestJSON = "Empty body"
		}
		/*
			// 如果请求体或响应体太长，可以只记录一部分
			if len(requestJSON) > 1000 {
				requestJSON = requestJSON[:1000] + "..."
			}
			if len(responseJSON) > 1000 {
				responseJSON = responseJSON[:1000] + "..."
			}
		*/

		userID, ok := c.Get("userid")
		if !ok {
			logService.LogWriter.WriteError("获取用户ID失败", false)
			return
		}
		var logInfo control.TLogControl
		logInfo.OperatorID = userID.(int32)
		//logInfo.LogTime = time.Now()
		logInfo.LatencyTime = latencyTime
		logInfo.ClientIP = clientIP
		logInfo.StatusCode = statusCode
		logInfo.ReqMethod = reqMethod
		logInfo.ReqUri = reqUri
		logInfo.RequestJson = requestJSON
		logInfo.ResponseJson = responseJSON
		if err := logInfo.InsertLog(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("写入日志失败：%s", err.Error()), false)
		}
	}
}

type responseBodyWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (r responseBodyWriter) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}
