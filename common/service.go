package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
)

var CurrentPath string

type GinContext struct {
	ctx *gin.Context
}

func NewGinContext(ctx *gin.Context) *GinContext {
	return &GinContext{ctx: ctx}
}

func (g *GinContext) CheckRequest(target any) error {
	if err := g.ctx.ShouldBind(target); err != nil {
		return err
	}
	data, err := json.Marshal(target)
	if err != nil {
		return err
	}
	LogServ.Debug(string(data))
	return nil
}

func (g *GinContext) GetOperator() (int32, string, error) {
	var val any
	var exists bool
	var ok bool
	var id int32
	var account string
	//get userid
	if val, exists = g.ctx.Get("userid"); !exists {
		return 0, "", errors.New("无权操作")
	}
	if id, ok = val.(int32); !ok {
		return 0, "", errors.New("userid类型错误")
	}
	//get account
	if val, exists = g.ctx.Get("account"); !exists {
		return 0, "", errors.New("无权操作")
	}
	if account, ok = val.(string); !ok {
		return 0, "", errors.New("account 类型错误")
	}
	return id, account, nil
}

func (g *GinContext) GetParam(key string) string {
	return g.ctx.Param(key)
}

func (g *GinContext) GetQuery(key string) string {
	return g.ctx.Query(key)
}

func (g *GinContext) GetHeader(key string) string {
	return g.ctx.GetHeader(key)
}

func (g *GinContext) Reply(isDebug bool, value any) {
	if isDebug {
		strJson, _ := json.Marshal(value)
		if isDebug {
			LogServ.Debug(string(strJson))
		}

	}
	g.ctx.JSON(200, value)
}
func (g *GinContext) ReplyBadRequest(isDebug bool, value any) {
	if isDebug {
		strJson, _ := json.Marshal(value)
		if isDebug {
			fmt.Println(string(strJson))
			LogServ.Debug(string(strJson))
		}

	}
	g.ctx.JSON(400, value)
}

func SetHeader(ctx *gin.Context) {
	ctx.Header("Access-Control-Allow-Origin", "*")
	ctx.Header("Access-Control-Allow-Methods", "POST")
	ctx.Header("X-Content-Type-Options", "nosniff")
	ctx.Header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization")
	ctx.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Cache-Control, Content-Language, Content-Type")
	ctx.Header("Access-Control-Allow-Credentials", "true")
	ctx.Next()
}
func CheckRequest(ctx *gin.Context, target interface{}) error {
	err := ctx.ShouldBind(target)
	if err != nil {
		return err
	}
	_, err = json.Marshal(target)
	if err != nil {
		return err
	}
	LogServ.Debug(target)
	return nil
}
func GetOperater(ctx *gin.Context) (int32, string, error) {
	var val any
	var exists bool
	var ok bool
	var id int32
	var account string
	//get userid
	if val, exists = ctx.Get("userid"); !exists {
		return 0, "", errors.New("无权操作")
	}
	if id, ok = val.(int32); !ok {
		return 0, "", errors.New("userid类型错误")
	}
	//get account
	if val, exists = ctx.Get("account"); !exists {
		return 0, "", errors.New("无权操作")
	}
	if account, ok = val.(string); !ok {
		return 0, "", errors.New("account 类型错误")
	}
	return id, account, nil
}

/*
func DownloadFile(ctx *gin.Context) {
	content := "Download file here happliy"
	fileName := "hello.txt"
	ctx.Header("Content-Disposition", "attachment; filename="+fileName)
	ctx.Header("Content-Type", "application/text/plain")
	ctx.Header("Accept-Length", fmt.Sprintf("%d", len(content)))
	ctx.Writer.Write([]byte(content))
	ctx.JSON(http.StatusOK, gin.H{
		"msg": "Download file successfully",
	})
}
*/
