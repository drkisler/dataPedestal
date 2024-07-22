package service

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	"github.com/drkisler/dataPedestal/plugin/servicePlugin/pub_service/control"
	"github.com/drkisler/dataPedestal/universal/messager"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"os"
	"os/signal"
	"sync"
)

type TBasePlugin = pluginBase.TBasePlugin

type TPublishPlugin struct {
	TBasePlugin
	ExitChan chan uint8
	//广播的协议在一般的路由器中默认是关闭的，需要手工开启，所以这里取消使用该协议，避免实施上面的问题
	//由此带来设计上的复杂性
	// PubSock     mangos.Socket
	ReplyServ *messager.TMessageServer
}

// Load 根据配置信息设置属性，创建必要的变量
func (mp *TPublishPlugin) Load(config string) common.TResponse {
	if mp == nil {
		return *common.Failure("plugin 初始化失败，不能加载")
	}
	var err error
	if resp := mp.TBasePlugin.Load(config); resp.Code < 0 {
		mp.Logger.WriteError(resp.Info)
		return resp
	}
	var cfg initializers.TPublishConfig
	if err = cfg.LoadConfig(config); err != nil {
		mp.Logger.WriteError(err.Error())
		return *common.Failure(err.Error())
	}
	if _, err = metaDataBase.GetDbServ(metaDataBase.Publishddl, metaDataBase.Grantddl); err != nil {
		mp.Logger.WriteError(err.Error())
		return *common.Failure(err.Error())
	}
	if err = control.InitGrantUser(); err != nil {
		mp.Logger.WriteError(err.Error())
		return *common.Failure(err.Error())
	}

	/*
		err = json.Unmarshal([]byte(config), &cfg)
		if err != nil {
			mp.Logger.WriteError(err.Error())
			return *common.Failure(err.Error())
		}*/
	if err = cfg.CheckValid(); err != nil {
		mp.Logger.WriteError(err.Error())
		return *common.Failure(err.Error())
	}

	/*
		if mp.PubSock, err = pub.NewSocket(); err != nil {
			mp.Logger.WriteError(err.Error())
			return *common.Failure(err.Error())
		}
		if err = mp.PubSock.Listen(cfg.PublishUrl); err != nil {
			mp.Logger.WriteError(err.Error())
			return *common.Failure(err.Error())
		}
	*/

	if mp.ReplyServ, err = messager.NewMessageServer(cfg.ReplyUrl, mp.HandleRequest); err != nil {
		mp.Logger.WriteError(err.Error())
		return *common.Failure(err.Error())
	}

	mp.Logger.WriteInfo("插件加载成功")
	//需要返回端口号，如果没有则返回1
	//return *common.ReturnInt(int(cfg.ServerPort))
	return *common.ReturnInt(1)
}

// GetConfigTemplate 向客户端返回配置信息的样例
func (mp *TPublishPlugin) GetConfigTemplate() common.TResponse {
	var cfg initializers.TPublishConfig
	cfg.SetDefault()
	data, err := json.Marshal(&cfg)
	if err != nil {
		return *common.Failure(err.Error())
	}
	return common.TResponse{Code: 0, Info: string(data)}
}

// Run 启动程序，启动前必须先Load
func (mp *TPublishPlugin) Run() common.TResponse {
	defer mp.SetRunning(false)
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		mp.ReplyServ.Start()
	}(&wg)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt) //注册相关信号的接受器
	mp.Logger.WriteInfo("插件已启动")
	//并发等待信号
	select {
	case <-quit: //操作系统发出退出信号
		mp.cleanup()
	case <-mp.ExitChan:
		mp.cleanup()
	}
	if err := control.UpdateGrantStatus(); err != nil {
		mp.Logger.WriteError(err.Error())
	}
	wg.Wait()
	mp.Logger.WriteInfo("插件已停止")
	return *common.Success(nil)
}
func (mp *TPublishPlugin) cleanup() {
	var closeOnce sync.Once
	closeOnce.Do(func() {
		mp.TBasePlugin.Stop()
		dbs, _ := metaDataBase.GetDbServ()
		_ = dbs.Close()
		mp.ReplyServ.Stop()
		close(mp.ExitChan)
	})
}

// Stop 手工停止
func (mp *TPublishPlugin) Stop() common.TResponse {
	mp.TBasePlugin.Stop()
	dbs, _ := metaDataBase.GetDbServ()
	if err := dbs.Close(); err != nil {
		return common.TResponse{Code: -1, Info: err.Error()}
	}
	mp.ReplyServ.Stop()
	mp.ExitChan <- 1                                              //通知线程退出
	return common.TResponse{Code: 0, Info: "success stop plugin"} //*common.Success(nil)
}

func (mp *TPublishPlugin) CustomInterface(pluginOperate common.TPluginOperate) common.TResponse {
	operateFunc, ok := operateMap[pluginOperate.OperateName]
	if !ok {
		return *common.Failure(fmt.Sprintf("接口 %s 不存在", pluginOperate.OperateName))
	}
	return operateFunc(pluginOperate.UserID, pluginOperate.Params)
}
func (mp *TPublishPlugin) HandleRequest(msg []byte) []byte {
	request, err := DecodeRequest(msg)
	if err != nil {
		return ReplyError(err.Error())
	}
	switch request.MessageType {
	case Request_Pull:
		var arrUUID []string
		if arrUUID, err = control.GetUpdatedPublish(int(request.UserID)); err != nil {
			return ReplyError(err.Error())
		}
		return ReplyData(arrUUID)
	case Request_Publish:
		if err = control.RenewPublishByUUID(request.UserID, request.publishUUID, request.MessageData); err != nil {
			return ReplyError(err.Error())
		}
		return ReplyOK()
	default:
		return ReplyError("message type not support")

	}

}
