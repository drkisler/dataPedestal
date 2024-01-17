package common

type TCheckJob struct {
	JobID   int32  `json:"job_id,omitempty"`
	JobName string `json:"job_name"`
	RunMode string `json:"run_mode,omitempty"`
	Cron    string `json:"cron,omitempty"`
	//Collects map[string]TTransCollect `json:"collects,omitempty"`
	Status TStatus `json:"-"`
}
