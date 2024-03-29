package common

type TCheckJob struct {
	JobID   int32   `json:"job_id,omitempty"`
	JobName string  `json:"job_name"`
	RunMode string  `json:"run_mode,omitempty"`
	Cron    string  `json:"cron,omitempty"`
	Status  TStatus `json:"-"`
}
