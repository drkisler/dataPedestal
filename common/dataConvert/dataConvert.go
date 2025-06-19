package dataConvert

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings" // 尽管优化了[]byte，但string转换时仍可能用到strings.TrimSpace
)

// 预定义错误，减少 fmt.Errorf 的开销
var (
	ErrUnsupportedType = errors.New("unsupported type")
	ErrOutOfRange      = errors.New("value out of range")
	ErrInvalidFloat    = errors.New("invalid float for integer conversion")
)

// trimSpaceBytes 辅助函数，高效地去除字节切片两端的空白字符
// 返回的 []byte 是原始切片的一个子切片，避免了新的内存分配。
func trimSpaceBytes(b []byte) []byte {
	first := 0
	for first < len(b) && isSpace(b[first]) {
		first++
	}
	last := len(b) - 1
	for last >= first && isSpace(b[last]) {
		last--
	}
	return b[first : last+1]
}

// isSpace 判断字节是否是空白字符
func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// toInt64 辅助函数，将输入值转换为 int64
func toInt64(val any) (int64, error) {
	switch v := val.(type) {
	case int64:
		return v, nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, ErrOutOfRange
		}
		return int64(v), nil
	case float32:
		f := float64(v)
		// 检查是否在int64范围内，以及是否是精确的整数
		if f < math.MinInt64 || f > math.MaxInt64 || float64(int64(f)) != f {
			return 0, ErrInvalidFloat
		}
		return int64(f), nil
	case float64:
		f := v
		// 检查是否在int64范围内，以及是否是精确的整数
		if f < math.MinInt64 || f > math.MaxInt64 || float64(int64(f)) != f {
			return 0, ErrInvalidFloat
		}
		return int64(f), nil
	case string:
		s := strings.TrimSpace(v)
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot parse %q as int64: %w", s, err)
		}
		return n, nil
	case []byte:
		// 先trim []byte，再转换为string进行解析，避免对整个原始字节切片转换为字符串再trim
		trimmedBytes := trimSpaceBytes(v)
		n, err := strconv.ParseInt(string(trimmedBytes), 10, 64)
		if err != nil {
			// 错误信息中仍可能涉及原始 []byte 到 string 的转换，但只在错误路径
			return 0, fmt.Errorf("cannot parse %q as int64: %w", string(v), err)
		}
		return n, nil
	default:
		return 0, ErrUnsupportedType
	}
}

// toUint64 辅助函数，将输入值转换为 uint64
func toUint64(val any) (uint64, error) {
	switch v := val.(type) {
	case uint64:
		return v, nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case int8:
		if v < 0 {
			return 0, ErrOutOfRange
		}
		return uint64(v), nil
	case int16:
		if v < 0 {
			return 0, ErrOutOfRange
		}
		return uint64(v), nil
	case int32:
		if v < 0 {
			return 0, ErrOutOfRange
		}
		return uint64(v), nil
	case int64:
		if v < 0 {
			return 0, ErrOutOfRange
		}
		return uint64(v), nil
	case float32:
		f := float64(v)
		// 检查是否在uint64范围内，以及是否是精确的整数
		if f < 0 || f > math.MaxUint64 || float64(uint64(f)) != f {
			return 0, ErrInvalidFloat
		}
		return uint64(f), nil
	case float64:
		f := v
		// 检查是否在uint64范围内，以及是否是精确的整数
		if f < 0 || f > math.MaxUint64 || float64(uint64(f)) != f {
			return 0, ErrInvalidFloat
		}
		return uint64(f), nil
	case string:
		s := strings.TrimSpace(v)
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot parse %q as uint64: %w", s, err)
		}
		return n, nil
	case []byte:
		// 先trim []byte，再转换为string进行解析
		trimmedBytes := trimSpaceBytes(v)
		n, err := strconv.ParseUint(string(trimmedBytes), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot parse %q as uint64: %w", string(v), err)
		}
		return n, nil
	default:
		return 0, ErrUnsupportedType
	}
}

// toFloat64 辅助函数，将输入值转换为 float64
func toFloat64(val any) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case string:
		s := strings.TrimSpace(v)
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot parse %q as float64: %w", s, err)
		}
		return f, nil
	case []byte:
		// 先trim []byte，再转换为string进行解析
		trimmedBytes := trimSpaceBytes(v)
		f, err := strconv.ParseFloat(string(trimmedBytes), 64)
		if err != nil {
			return 0, fmt.Errorf("cannot parse %q as float64: %w", string(v), err)
		}
		return f, nil
	default:
		return 0, ErrUnsupportedType
	}
}

// ToInt8 转换为 int8
func ToInt8(val any) (int8, error) {
	n, err := toInt64(val)
	if err != nil {
		return 0, err
	}
	if n < math.MinInt8 || n > math.MaxInt8 {
		return 0, ErrOutOfRange
	}
	return int8(n), nil
}

// ToUint8 转换为 uint8
func ToUint8(val any) (uint8, error) {
	n, err := toUint64(val)
	if err != nil {
		return 0, err
	}
	if n > math.MaxUint8 {
		return 0, ErrOutOfRange
	}
	return uint8(n), nil
}

// ToInt16 转换为 int16
func ToInt16(val any) (int16, error) {
	n, err := toInt64(val)
	if err != nil {
		return 0, err
	}
	if n < math.MinInt16 || n > math.MaxInt16 {
		return 0, ErrOutOfRange
	}
	return int16(n), nil
}

// ToUint16 转换为 uint16
func ToUint16(val any) (uint16, error) {
	n, err := toUint64(val)
	if err != nil {
		return 0, err
	}
	if n > math.MaxUint16 {
		return 0, ErrOutOfRange
	}
	return uint16(n), nil
}

// ToInt32 转换为 int32
func ToInt32(val any) (int32, error) {
	n, err := toInt64(val)
	if err != nil {
		return 0, err
	}
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, ErrOutOfRange
	}
	return int32(n), nil
}

// ToUint32 转换为 uint32
func ToUint32(val any) (uint32, error) {
	n, err := toUint64(val)
	if err != nil {
		return 0, err
	}
	if n > math.MaxUint32 {
		return 0, ErrOutOfRange
	}
	return uint32(n), nil
}

// ToInt64 转换为 int64
func ToInt64(val any) (int64, error) {
	return toInt64(val)
}

// ToUint64 转换为 uint64
func ToUint64(val any) (uint64, error) {
	return toUint64(val)
}

// ToFloat32 转换为 float32
func ToFloat32(val any) (float32, error) {
	f, err := toFloat64(val)
	if err != nil {
		return 0, err
	}
	// 检查是否在 float32 的可表示范围内，超出则视为溢出
	if f < -math.MaxFloat32 || f > math.MaxFloat32 { // 注意：此处没有考虑Subnormal numbers
		return 0, ErrOutOfRange
	}
	return float32(f), nil
}

// ToFloat64 转换为 float64
func ToFloat64(val any) (float64, error) {
	return toFloat64(val)
}

// ToString 优化后的 string 转换函数（直接重用 strconv，避免反射）
func ToString(val any) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int: // Go int 的大小取决于系统架构，一般是int32或int64
		return strconv.FormatInt(int64(v), 10), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint: // Go uint 的大小取决于系统架构，一般是uint32或uint64
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool: // 新增布尔类型转换
		return strconv.FormatBool(v), nil
	default:
		// 对于其他未明确处理的类型，返回错误，避免反射开销
		return "", ErrUnsupportedType
	}
}

func CastAs[T any](val any) (T, error) {
	var zero T
	switch any(zero).(type) {
	case int8:
		v, err := ToInt8(val)
		return any(v).(T), err
	case int16:
		v, err := ToInt16(val)
		return any(v).(T), err
	case int32:
		v, err := ToInt32(val)
		return any(v).(T), err
	case int64:
		v, err := ToInt64(val)
		return any(v).(T), err
	case int:
		v, err := ToInt64(val)
		return any(int(v)).(T), err
	case uint8:
		v, err := ToUint8(val)
		return any(v).(T), err
	case uint16:
		v, err := ToUint16(val)
		return any(v).(T), err
	case uint32:
		v, err := ToUint32(val)
		return any(v).(T), err
	case uint64:
		v, err := ToUint64(val)
		return any(v).(T), err
	case uint:
		v, err := ToUint64(val)
		return any(uint(v)).(T), err
	case float32:
		v, err := ToFloat32(val)
		return any(v).(T), err
	case float64:
		v, err := ToFloat64(val)
		return any(v).(T), err
	case string:
		v, err := ToString(val)
		return any(v).(T), err
	default:
		return zero, ErrUnsupportedType
	}
}
