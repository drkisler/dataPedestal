package common

import (
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
)

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
