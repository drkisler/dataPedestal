package common

type TLogInfo struct {
	LogID   int64  `json:"log_id,omitempty"`
	LogDate string `json:"log_date,omitempty"`
	LogTime string `json:"log_time,omitempty"`
	LogInfo string `json:"log_info,omitempty"`
}

type TLogQuery struct {
	LogDate   string `json:"log_date"`
	PageSize  int32  `json:"page_size,omitempty"`
	PageIndex int32  `json:"page_index,omitempty"`
}
