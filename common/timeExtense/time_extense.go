package timeExtense

import (
	"fmt"
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
