package response

const (
	RespSuccess int64 = 0
	RespFailure int64 = -1
)

type TRespDataSet struct {
	Total   int64       `json:"total"`
	ArrData interface{} `json:"list,omitempty"`
}
type TResponse struct {
	Code int64         `json:"code"`
	Data *TRespDataSet `json:"data"` //,omitempty
	Info string        `json:"message"`
}

func Failure(info string) *TResponse {
	var resp TResponse
	resp.Code = RespFailure
	resp.Data = nil
	resp.Info = info
	return &resp
}

func ReturnInt(value int64) *TResponse {
	return &TResponse{value, nil, "success"}
}
func ReturnStr(value string) *TResponse {
	return &TResponse{RespSuccess, nil, value}
}
func Success(data *TRespDataSet) *TResponse {

	return &TResponse{RespSuccess, data, "success"}
}

func Ongoing() *TResponse {
	return &TResponse{1, nil, "ongoing"}
}

func RespData(total int64, data interface{}, err error) *TResponse {
	if err != nil {
		return Failure(err.Error())
	}
	var dataSet TRespDataSet
	dataSet.Total = total
	dataSet.ArrData = data
	return Success(&dataSet)
}
