package module

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"time"
)

type TPortalLog struct {
	LogID        int64      `json:"log_id"`
	LogTime      *time.Time `json:"log_time,omitempty"`
	LatencyTime  string     `json:"latency_time,omitempty"`
	ClientIP     string     `json:"client_ip,omitempty"`
	StatusCode   string     `json:"status_code,omitempty"`
	ReqMethod    string     `json:"req_method,omitempty"`
	ReqUri       string     `json:"req_uri,omitempty"`
	RequestJson  string     `json:"request_json,omitempty"`
	ResponseJson string     `json:"response_json,omitempty"`
}

func (p *TPortalLog) InsertLog(userid int32) error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	ctx := context.Background()
	if err = storage.ExecuteSQL(ctx, fmt.Sprintf("with cet_id as(select coalesce(max(log_id),0)log_id from %s.portal_log where user_id=$1)INSERT "+
		"INTO %s.portal_log(log_id,user_id, log_time, latency_time, client_ip, status_code,req_method, req_uri, request_json, response_json)"+
		"select log_id+1,$2, now(), $3, $4, $5, $6, $7, $8, $9 "+
		"from cet_id", storage.GetSchema(), storage.GetSchema()),
		userid,
		userid,
		//time.Now().Format("2006-01-02 15:04:05"),
		p.LatencyTime,
		p.ClientIP,
		p.StatusCode,
		p.ReqMethod,
		p.ReqUri,
		p.RequestJson,
		p.ResponseJson,
	); err != nil {
		return err
	}
	return nil
}

func (p *TPortalLog) GetLogs(userid, pageSize, pageIndex int32) ([]TPortalLog, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}
	strSQL := fmt.Sprintf("select log_id,log_time,latency_time,client_ip,status_code,req_method,req_uri,request_json,response_json "+
		"from %s.portal_log where user_id=$1 order by log_id desc limit %d offset %d", storage.GetSchema(), pageSize, (pageIndex-1)*pageSize)

	rows, err := storage.QuerySQL(strSQL, userid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var logs []TPortalLog
	for rows.Next() {
		var log TPortalLog
		err = rows.Scan(&log.LogID, &log.LogTime, &log.LatencyTime, &log.ClientIP, &log.StatusCode, &log.ReqMethod, &log.ReqUri, &log.RequestJson, &log.ResponseJson)
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	//fmt.Println(len(logs))

	return logs, nil
}

func (p *TPortalLog) DeleteLogs(userid int32) error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("delete "+
		"from %s.portal_log where user_id=$1 and log_id=$2", storage.GetSchema())
	ctx := context.Background()
	err = storage.ExecuteSQL(ctx, strSQL, userid, p.LogID)
	if err != nil {
		return err
	}
	return nil
}

func (p *TPortalLog) ClearLogs(userid int32) error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("delete "+
		"from %s.portal_log where user_id=$1 ", storage.GetSchema())
	ctx := context.Background()
	err = storage.ExecuteSQL(ctx, strSQL, userid)
	if err != nil {
		return err
	}
	return nil
}
