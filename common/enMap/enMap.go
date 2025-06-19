package enMap

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// 泛型函数：根据目标类型 T 提取对应值
func ExtractValueFromMap[T any](key string, data map[string]interface{}) (T, error) {
	var zero T
	if data == nil {
		return zero, fmt.Errorf("map is nil")
	}
	val, ok := data[key]
	if !ok {
		return zero, nil
	}

	// 使用类型断言来匹配目标类型
	switch any(zero).(type) {
	case int:
		v, err := convertToInt(val)
		return any(v).(T), err
	case int32:
		v, err := convertToInt32(val)
		return any(v).(T), err
	case int64:
		v, err := convertToInt64(val)
		return any(v).(T), err
	case float64:
		v, err := convertToFloat64(val)
		return any(v).(T), err
	case string:
		v, err := convertToString(val)
		return any(v).(T), err
	case bool:
		v, err := convertToBool(val)
		return any(v).(T), err
	case time.Time:
		v, err := convertToTime(val)
		return any(v).(T), err
	default:
		return zero, fmt.Errorf("unsupported type for key %q", key)
	}
}

func convertToInt(val interface{}) (int, error) {
	switch v := val.(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		// int64 与 int 在 64 位架构下范围相同，无需额外检查
		return int(v), nil
	case float64:
		if v > 9223372036854775807.0 || v < -9223372036854775808.0 {
			return 0, fmt.Errorf("float64 value out of int range: %f", v)
		}
		if v != float64(int64(v)) { // 检查是否会因截断丢失精度
			return 0, fmt.Errorf("float64 value cannot be accurately converted to int: %f", v)
		}
		return int(v), nil
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0, fmt.Errorf("cannot convert string to int: %s", err)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", val)
	}
}

func convertToInt32(val interface{}) (int32, error) {
	switch v := val.(type) {
	case int:
		if v > 2147483647 || v < -2147483648 {
			return 0, fmt.Errorf("int value out of int32 range: %d", v)
		}
		return int32(v), nil
	case int32:
		return v, nil
	case int64:
		if v > 2147483647 || v < -2147483648 {
			return 0, fmt.Errorf("int64 value out of int32 range: %d", v)
		}
		return int32(v), nil
	case float64:
		if v > 2147483647.0 || v < -2147483648.0 {
			return 0, fmt.Errorf("float64 value out of int32 range: %f", v)
		}
		if v != float64(int32(v)) { // 检查是否会因截断丢失精度
			return 0, fmt.Errorf("float64 value cannot be accurately converted to int32: %f", v)
		}
		return int32(v), nil
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 32)
		return int32(i), err
	default:
		return 0, fmt.Errorf("cannot convert %T to int32", val)
	}
}

func convertToInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		if v > float64(^uint64(0)>>1) || v < -float64(^uint64(0)>>1) {
			return 0, fmt.Errorf("float64 value out of int64 range: %f", v)
		}
		return int64(v), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", val)
	}
}

func convertToFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(strings.TrimSpace(v), 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", val)
	}
}

func convertToString(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case int, int32, int64, float32, float64, bool:
		return fmt.Sprintf("%v", v), nil
	default:
		return "", fmt.Errorf("cannot convert %T to string", val)
	}
}

func convertToBool(val interface{}) (bool, error) {
	switch v := val.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(strings.TrimSpace(v))
	default:
		return false, fmt.Errorf("cannot convert %T to bool", val)
	}
}

func convertToTime(val interface{}) (time.Time, error) {
	switch v := val.(type) {
	case time.Time:
		return v, nil
	case int64:
		return time.Unix(v, 0), nil
	case float64:
		return time.Unix(int64(v), 0), nil
	case string:
		layouts := []string{
			time.RFC3339,
			time.RFC3339Nano,
			time.RFC3339,
			time.RFC822,
			time.RFC850,
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05.999",
			"2006-01-02 15:04:05",
			"2006-01-02",
			"2006-01-02T15:04:05",
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02T15:04:05Z",
			"2006年01月02日 15时04分05秒",
			"2006年01月02日15时04分05秒",
		}
		s := strings.TrimSpace(v)
		for _, layout := range layouts {
			if t, err := time.Parse(layout, s); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("invalid time string format: %q", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", val)
	}
}
