package common

import (
	"encoding/json"
	"strconv"
	"time"
)

type DataType string

const (
	String    DataType = "string"
	Integer   DataType = "integer"
	Float     DataType = "float"
	Date      DataType = "date"
	Datetime  DataType = "datetime"
	Timestamp DataType = "timestamp"
)
const TimeStampColumn = "pull_time"

type FilterValue struct {
	Column string      `json:"column"`
	Value  interface{} `json:"value"`
}
type FilterCondition struct {
	Column   string   `json:"column"`
	DataType DataType `json:"dataType"`
	Value    string   `json:"value"`
}

func FilterConditionsToJSON(conditions []FilterCondition) (string, error) {
	jsonData, err := json.Marshal(conditions)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

func JSONToFilterConditions(jsonString *string) ([]FilterCondition, error) {
	var conditions []FilterCondition
	err := json.Unmarshal([]byte(*jsonString), &conditions)
	if err != nil {
		return nil, err
	}
	return conditions, nil
}

func JSONToFilterValues(jsonString *string) ([]FilterValue, error) {
	var conditions []FilterCondition
	err := json.Unmarshal([]byte(*jsonString), &conditions)
	if err != nil {
		return nil, err
	}
	var result []FilterValue
	for _, condition := range conditions {
		var value interface{}
		switch condition.DataType {
		case String:
			value = condition.Value
		case Integer:
			value, err = strconv.Atoi(condition.Value)
			if err != nil {
				return nil, err
			}
		case Float:
			value, err = strconv.ParseFloat(condition.Value, 64)
			if err != nil {
				return nil, err
			}
		case Date:
			value, err = time.Parse("2006-01-02", condition.Value)
			if err != nil {
				return nil, err
			}
		case Datetime:
			value, err = time.Parse("2006-01-02 15:04:05", condition.Value)
			if err != nil {
				return nil, err
			}
		case Timestamp:
			value, err = time.Parse("2006-01-02 15:04:05.999999999", condition.Value)
			if err != nil {
				return nil, err
			}
		}
		result = append(result, FilterValue{
			Column: condition.Column,
			Value:  value,
		})
	}

	return result, nil
}
