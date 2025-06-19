package timeExtense

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func TimeSpent(iStartTime, iStopTime int64) string {
	start := time.Unix(iStartTime, 0)
	stop := time.Unix(iStopTime, 0)
	if start.IsZero() || stop.IsZero() {
		return ""
	}
	if start.After(stop) {
		start, stop = stop, start
	}

	duration := stop.Sub(start)
	hours := int(duration.Hours())
	minutes := int(duration.Minutes()) % 60
	seconds := int(duration.Seconds()) % 60

	var parts []string

	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%d时", hours))
	}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%d分", minutes))
	}
	if seconds > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%d秒", seconds))
	}
	return strings.Join(parts, "")
}

func ConvertToTime(val interface{}) (time.Time, error) {
	switch v := val.(type) {
	case time.Time:
		return v, nil
	case []uint8:
		return parseWithLayouts(string(v))
	case string:
		// 尝试解析为时间戳
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
			if ts > 1e12 {
				return time.UnixMilli(ts), nil
			}
			return time.Unix(ts, 0), nil
		}
		return parseWithLayouts(v)
	case int64:
		return time.Unix(v, 0), nil
	case int:
		return time.Unix(int64(v), 0), nil
	case float64:
		return time.Unix(int64(v), 0), nil
	default:
		return time.Time{}, fmt.Errorf("不支持将类型 %T 转换为 time.Time", val)
	}
}

func parseWithLayouts(s string) (time.Time, error) {
	for _, layout := range knownLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("无法将值 %q 解析为时间格式", s)
}

var knownLayouts = []string{
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
