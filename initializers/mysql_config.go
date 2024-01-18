package initializers

type TMySQLConfig struct {
	TConfigure
	ConnectString string `json:"connect_string"`
	DestDatabase  string `json:"dest_database"`
	KeepConnect   bool   `json:"keep_connect"`
	ConnectBuffer int    `json:"connect_buffer"`
	DataBuffer    int    `json:"data_buffer"`
	SkipHour      []int  `json:"skip_hour"`
	Frequency     int    `json:"frequency"`
	ServerPort    int32  `json:"server_port"`
}
