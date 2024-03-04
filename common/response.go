package common

type TRespDataSet struct {
	Total   int32       `json:"total"`
	ArrData interface{} `json:"list,omitempty"`
}
type TResponse struct {
	Code int32         `json:"code"`
	Data *TRespDataSet `json:"data,omitempty"`
	Info string        `json:"message"`
}

func Failure(info string) *TResponse {
	var resp TResponse
	resp.Code = -1
	resp.Data = nil
	resp.Info = info
	return &resp
}

func ReturnInt(value int) *TResponse {
	return &TResponse{int32(value), nil, "success"}
}
func ReturnStr(value string) *TResponse {
	return &TResponse{0, nil, value}
}
func Success(data *TRespDataSet) *TResponse {
	return &TResponse{0, data, "success"}
}
func RespData(total int32, data interface{}, err error) *TResponse {
	if err != nil {
		return Failure(err.Error())
	}
	var dataSet TRespDataSet
	dataSet.Total = total
	dataSet.ArrData = data
	return Success(&dataSet)
}
