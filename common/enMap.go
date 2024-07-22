package common

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func GetInt32ValueFromMap(key string, data map[string]interface{}) (int32, error) {
	iValue, err := GetIntValueFromMap(key, data)
	if err != nil {
		return 0, err
	}
	return int32(iValue), nil
}

func GetIntValueFromMap(key string, data map[string]interface{}) (int, error) {
	if data == nil {
		return 0, fmt.Errorf("data is nil")
	}
	if value, ok := data[key]; ok {
		if intValue, ok := value.(int); ok {
			return intValue, nil
		} else if int32Value, ok := value.(int32); ok {
			return int(int32Value), nil
		} else if float64Value, ok := value.(float64); ok {
			return int(float64Value), nil
		} else if int64Value, ok := value.(int64); ok {
			return int(int64Value), nil
		} else {
			return 0, fmt.Errorf("value is not int or int32 or float64 or int64  by key %s", key)
		}
	}
	return 0, nil
}

func GetInt64ValueFromMap(key string, data map[string]interface{}) (int64, error) {
	if data == nil {
		return 0, fmt.Errorf("data is nil")
	}
	if value, ok := data[key]; ok {
		if int64Value, ok := value.(int64); ok {
			return int64Value, nil
		} else if int32Value, ok := value.(int32); ok {
			return int64(int32Value), nil
		} else {
			return 0, fmt.Errorf("value is not int64 or int32  by key %s", key)
		}
	}
	return 0, nil
}

func GetFloat64ValueFromMap(key string, data map[string]interface{}) (float64, error) {
	if data == nil {
		return 0, fmt.Errorf("data is nil")
	}
	if value, ok := data[key]; ok {
		if float64Value, ok := value.(float64); ok {
			return float64Value, nil
		} else if int32Value, ok := value.(int32); ok {
			return float64(int32Value), nil
		} else {
			return 0, fmt.Errorf("value is not float64 or int32  by key %s", key)
		}
	}
	return 0, nil
}

func GetStringValueFromMap(key string, data map[string]interface{}) (string, error) {
	if data == nil {
		return "", fmt.Errorf("data is nil")
	}
	if value, ok := data[key]; ok {
		if stringValue, ok := value.(string); ok {
			return stringValue, nil
		} else {
			return "", fmt.Errorf("value is not string  by key %s", key)
		}
	}
	return "", nil
}

func GetIntArrayFromMap(key string, data map[string]interface{}) ([]int, error) {
	if data == nil {
		return nil, fmt.Errorf("data is nil")
	}
	var strValue string
	var err error
	if value, ok := data[key]; ok {
		if strValue, ok = value.(string); ok {
			strTmp := strings.Split(strValue, ",")
			result := make([]int, len(strTmp))
			for i, str := range strTmp {
				result[i], err = strconv.Atoi(str)
				if err != nil {
					return nil, fmt.Errorf("value is not int array  by key %s", key)
				}
			}
			return result, nil

		} else {
			return nil, fmt.Errorf("value is not string  by key %s", key)
		}
	}
	return nil, nil
}

func MapToString(data *map[string]interface{}) string {
	if data == nil {
		return ""
	}
	// return key1=value1,key2=value2,key3=value3
	var builder strings.Builder

	for key, value := range *data {
		builder.WriteString(fmt.Sprintf("%s=%v,", key, value))
	}
	// 删除最后一个逗号
	result := builder.String()
	if len(result) > 0 {
		result = result[:len(result)-1]
	}
	return result
}

func StringToMap(source *string) (map[string]string, error) {
	if source == nil {
		return nil, fmt.Errorf("source is nil")
	}
	var values map[string]interface{}
	err := json.Unmarshal([]byte(*source), &values)
	if err != nil {
		return nil, err
	}

	return ConvertToStrMap(values)

	/*
		for key, value := range values {
			switch v := value.(type) {
			case string:
				result[key] = v
			case float64:
				if math.Floor(v) == v {
					result[key] = strconv.FormatInt(int64(v), 10)
				} else {
					result[key] = strconv.FormatFloat(v, 'f', -1, 64)
				}
			case bool:
				result[key] = strconv.FormatBool(v)
			case nil:
				result[key] = ""
			case []interface{}:
				// 将数组转换为逗号分隔的字符串
				var strArr []string
				for _, item := range v {
					strArr = append(strArr, fmt.Sprintf("%v", item))
				}
				result[key] = strings.Join(strArr, ",")
			default:
				return nil, fmt.Errorf("value is not a supported type for key %s", key)
			}
		}

		return result, nil
	*/
}
func ConvertToStrMap(values map[string]interface{}) (map[string]string, error) {
	result := make(map[string]string, len(values))
	// 只考虑基本类型
	for key, value := range values {
		switch v := value.(type) {
		case int, int8, int16, int32, int64, float32, float64, string, bool:
			result[key] = fmt.Sprintf("%v", v)
		default:
			return nil, fmt.Errorf("value is not a supported type for key %s", key)
		}
	}
	return result, nil
}
