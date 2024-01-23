package clickHouse

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

// TBufferData 一列的缓存
type TBufferData struct {
	Data       any
	ColName    string
	ColumnType proto.ColumnType
	Precision  int64
	Scale      int64
	// func Initialize (proto.ColumnType) 初始化数据
	// func Append(any) 将数据写入缓存
	// func InPutData() proto.InputColumn  {Name string,Data ColInput}
}

func UInt128FromBin(val []byte) proto.UInt128 {
	_ = val[:128/8] // bounds check hint to compiler; see golang.org/issue/14808
	return proto.UInt128{
		Low:  binary.LittleEndian.Uint64(val[0 : 64/8]),
		High: binary.LittleEndian.Uint64(val[64/8 : 128/8]),
	}
}
func UInt128FromHex(val string) (proto.UInt128, error) {
	byteValues, err := hex.DecodeString(val)
	if err != nil {
		return proto.UInt128{}, errors.Errorf("failed to decode string : %v", val)
	}
	return UInt128FromBin(byteValues), nil
}
func UInt256FromBin(val []byte) proto.UInt256 {
	_ = val[:256/8] // bounds check hint to compiler; see golang.org/issue/14808
	// Calling manually because binUInt128 is not inlining.
	return proto.UInt256{
		Low: proto.UInt128{
			Low:  binary.LittleEndian.Uint64(val[0 : 64/8]),
			High: binary.LittleEndian.Uint64(val[64/8 : 128/8]),
		},
		High: proto.UInt128{
			Low:  binary.LittleEndian.Uint64(val[128/8 : 192/8]),
			High: binary.LittleEndian.Uint64(val[192/8 : 256/8]),
		},
	}
}
func UInt256FromHex(val string) (proto.UInt256, error) {
	byteValues, err := hex.DecodeString(val)
	if err != nil {
		return proto.UInt256{}, errors.Errorf("failed to decode string : %v", val)
	}
	return UInt256FromBin(byteValues), nil
}
func PointFromStr(val string) (proto.Point, error) {
	s := strings.Trim(strings.Join(strings.Fields(val), ""), "()")
	parts := strings.Split(s, ",")
	if len(parts) != 2 {
		return proto.Point{}, errors.Errorf("%v is not then point format", val)
	}
	x, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return proto.Point{}, errors.Errorf("%v is not then point format", val)
	}
	y, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return proto.Point{}, errors.Errorf("%v is not then point format", val)
	}
	return proto.Point{X: x, Y: y}, nil
}
func PadNumberWithZeros(number string, length int) string {
	var buffer bytes.Buffer
	buffer.WriteString(number)
	for buffer.Len() < length {
		buffer.WriteString("0")
	}
	return buffer.String()
}

func (bd *TBufferData) InPutData() proto.InputColumn {
	switch bd.ColumnType {
	case proto.ColumnTypeDecimal32, proto.ColumnTypeDecimal64, proto.ColumnTypeDecimal128, proto.ColumnTypeDecimal256:
		return proto.InputColumn{Name: bd.ColName, Data: proto.Alias(bd.Data.(proto.Column), proto.ColumnType(fmt.Sprintf("Decimal(%d,%d)", bd.Precision, bd.Scale)))}
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal32), proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal64),
		proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal128), proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal256):
		return proto.InputColumn{Name: bd.ColName, Data: proto.Alias(bd.Data.(proto.Column), proto.ColumnType(fmt.Sprintf("Nullable(Decimal(%d,%d))", bd.Precision, bd.Scale)))}
	default:
		return proto.InputColumn{Name: bd.ColName, Data: bd.Data.(proto.ColInput)}
	}
}

func (bd *TBufferData) Initialize(colName string, colType proto.ColumnType, precision ...int64) error {
	bd.ColumnType = colType
	bd.ColName = colName
	switch colType {
	case proto.ColumnTypeInt8:
		bd.Data = new(proto.ColInt8)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt8):
		bd.Data = new(proto.ColInt8).Nullable()
	case proto.ColumnTypeInt16:
		bd.Data = new(proto.ColInt16)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt16):
		bd.Data = new(proto.ColInt16).Nullable()
	case proto.ColumnTypeInt32:
		bd.Data = new(proto.ColInt32)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt32):
		bd.Data = new(proto.ColInt32).Nullable()
	case proto.ColumnTypeInt64:
		bd.Data = new(proto.ColInt64)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt64):
		bd.Data = new(proto.ColInt64).Nullable()
	case proto.ColumnTypeInt128:
		bd.Data = new(proto.ColInt128)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt128):
		bd.Data = new(proto.ColInt128).Nullable()
	case proto.ColumnTypeInt256:
		bd.Data = new(proto.ColInt256)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt256):
		bd.Data = new(proto.ColInt256).Nullable()
	case proto.ColumnTypeUInt8:
		bd.Data = new(proto.ColUInt8)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt8):
		bd.Data = new(proto.ColUInt8).Nullable()
	case proto.ColumnTypeUInt16:
		bd.Data = new(proto.ColUInt16)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt16):
		bd.Data = new(proto.ColUInt16).Nullable()
	case proto.ColumnTypeUInt32:
		bd.Data = new(proto.ColUInt32)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt32):
		bd.Data = new(proto.ColUInt32).Nullable()
	case proto.ColumnTypeUInt64:
		bd.Data = new(proto.ColUInt64)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt64):
		bd.Data = new(proto.ColUInt64).Nullable()
	case proto.ColumnTypeUInt128:
		bd.Data = new(proto.ColUInt128)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt128):
		bd.Data = new(proto.ColUInt128).Nullable()
	case proto.ColumnTypeUInt256:
		bd.Data = new(proto.ColUInt256)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt256):
		bd.Data = new(proto.ColUInt256).Nullable()
	case proto.ColumnTypeFloat32:
		bd.Data = new(proto.ColFloat32)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat32):
		bd.Data = new(proto.ColFloat32).Nullable()
	case proto.ColumnTypeFloat64:
		bd.Data = new(proto.ColFloat64)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat64):
		bd.Data = new(proto.ColFloat64).Nullable()
	case proto.ColumnTypeString:
		bd.Data = new(proto.ColStr)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeString):
		bd.Data = new(proto.ColStr).Nullable()
	case proto.ColumnTypeFixedString:
		bd.Data = new(proto.ColFixedStr)
	case proto.ColumnTypeArray:
		bd.Data = new(proto.ColArr[string])
	case proto.ColumnTypeIPv4:
		bd.Data = new(proto.ColIPv4)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeIPv4):
		bd.Data = new(proto.ColIPv4).Nullable()
	case proto.ColumnTypeIPv6:
		bd.Data = new(proto.ColIPv6)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeIPv6):
		bd.Data = new(proto.ColIPv6).Nullable()
	case proto.ColumnTypeDateTime:
		bd.Data = new(proto.ColDateTime)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDateTime):
		bd.Data = new(proto.ColDateTime).Nullable()
	case proto.ColumnTypeDateTime64:
		if len(precision) > 0 {
			bd.Data = new(proto.ColDateTime64).WithPrecision(proto.Precision(precision[0]))
		} else {
			bd.Data = new(proto.ColDateTime64).WithPrecision(proto.Precision(0))
		}
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDateTime64):
		if len(precision) > 0 {
			bd.Data = proto.NewColNullable[time.Time](new(proto.ColDateTime64).WithPrecision(proto.Precision(precision[0])))
		} else {
			bd.Data = proto.NewColNullable[time.Time](new(proto.ColDateTime64).WithPrecision(proto.Precision(0)))
		}
	case proto.ColumnTypeDate:
		bd.Data = new(proto.ColDate)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDate):
		bd.Data = new(proto.ColDate).Nullable()
	case proto.ColumnTypeDate32:
		bd.Data = new(proto.ColDate32)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDate32):
		bd.Data = new(proto.ColDate32).Nullable()
	case proto.ColumnTypeUUID:
		bd.Data = new(proto.ColUUID)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUUID):
		bd.Data = new(proto.ColUUID).Nullable()
	case proto.ColumnTypeEnum8:
		bd.Data = new(proto.ColEnum8)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeEnum8):
		bd.Data = new(proto.ColEnum8).Nullable()
	case proto.ColumnTypeEnum16:
		bd.Data = new(proto.ColEnum16)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeEnum16):
		bd.Data = new(proto.ColEnum16).Nullable()
	case proto.ColumnTypeMap:
		bd.Data = new(proto.ColMap[string, string])
	case proto.ColumnTypeBool:
		bd.Data = new(proto.ColBool)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeBool):
		bd.Data = new(proto.ColBool).Nullable()
	case proto.ColumnTypeTuple:
		bd.Data = new(proto.ColTuple)
	case proto.ColumnTypeDecimal32:
		bd.Data = new(proto.ColDecimal32)
		if len(precision) != 2 {
			return errors.Errorf("decimal type must have precision and scale define")
		}
		bd.Precision = precision[0]
		bd.Scale = precision[1]
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal32):
		bd.Data = new(proto.ColDecimal32).Nullable()
		if len(precision) != 2 {
			return errors.Errorf("decimal type must have precision and scale define")
		}
		bd.Precision = precision[0]
		bd.Scale = precision[1]
	case proto.ColumnTypeDecimal64:
		bd.Data = new(proto.ColDecimal64)
		if len(precision) != 2 {
			return errors.Errorf("decimal type must have precision and scale define")
		}
		bd.Precision = precision[0]
		bd.Scale = precision[1]
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal64):
		bd.Data = new(proto.ColDecimal64).Nullable()
		if len(precision) != 2 {
			return errors.Errorf("decimal type must have precision and scale define")
		}
		bd.Precision = precision[0]
		bd.Scale = precision[1]
	case proto.ColumnTypeDecimal128:
		bd.Data = new(proto.ColDecimal128)
		if len(precision) != 2 {
			return errors.Errorf("decimal type must have precision and scale define")
		}
		bd.Precision = precision[0]
		bd.Scale = precision[1]
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal128):
		bd.Data = new(proto.ColDecimal128).Nullable()
		if len(precision) != 2 {
			return errors.Errorf("decimal type must have precision and scale define")
		}
		bd.Precision = precision[0]
		bd.Scale = precision[1]
	case proto.ColumnTypeDecimal256:
		bd.Data = new(proto.ColDecimal256)
		if len(precision) != 2 {
			return errors.Errorf("decimal type must have precision and scale define")
		}
		bd.Precision = precision[0]
		bd.Scale = precision[1]
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal256):
		bd.Data = new(proto.ColDecimal256).Nullable()
		if len(precision) != 2 {
			return errors.Errorf("decimal type must have precision and scale define")
		}
		bd.Precision = precision[0]
		bd.Scale = precision[1]
	case proto.ColumnTypePoint:
		bd.Data = new(proto.ColPoint)
	case proto.ColumnTypeInterval:
		bd.Data = new(proto.ColInterval)
	case proto.ColumnTypeNothing:
		bd.Data = new(proto.ColNothing)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeNothing):
		bd.Data = new(proto.ColNothing).Nullable()
	default:
		return errors.Errorf("clickHouse not supported for %s", colType)
	}

	return nil
}
func (bd *TBufferData) Append(val any) error {
	var convertDecimalToNumber = func(value string) (int64, error) {
		arrStr := strings.Split(value, ".")
		if len(arrStr) == 2 {
			vInt := arrStr[0]
			vDec := arrStr[1]
			value = vInt + PadNumberWithZeros(vDec, int(bd.Scale))
		} else if len(arrStr) > 2 {
			return 0, errors.Errorf("%v is not valid decimal", val)
		}
		result, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, errors.Errorf("%v is not the type int32 or int64", val)
		}
		return result, nil
	}
	switch bd.ColumnType {
	case proto.ColumnTypeNothing:
		v, o := val.(proto.Nothing)
		if !o {
			return errors.Errorf("%v is not the type proto.Nothing", val)
		}
		bd.Data.(*proto.ColNothing).Append(v)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeNothing):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.Nothing]).Append(proto.Null[proto.Nothing]())
		} else {
			v, o := val.(proto.Nothing)
			if !o {
				return errors.Errorf("%v is not the type proto.Nothing", val)
			}
			bd.Data.(*proto.ColNullable[proto.Nothing]).Append(proto.NewNullable[proto.Nothing](v))
		}
	case proto.ColumnTypeInt8:
		value, ok := val.(int8)
		if !ok {
			return errors.Errorf("%v is not the type int8", val)
		}
		bd.Data.(*proto.ColInt8).Append(value)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt8):
		if val == nil {
			bd.Data.(*proto.ColNullable[int8]).Append(proto.Null[int8]())
		} else {
			value, ok := val.(int8)
			if !ok {
				return errors.Errorf("%v is not the type int8", val)
			}
			bd.Data.(*proto.ColNullable[int8]).Append(proto.NewNullable[int8](value))
		}
	case proto.ColumnTypeInt16:
		value, ok := val.(int16)
		if !ok {
			return errors.Errorf("%v is not the type int16", val)
		}
		bd.Data.(*proto.ColInt16).Append(value)
		//bd.Data = new(proto.ColInt16)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt16):
		if val == nil {
			bd.Data.(*proto.ColNullable[int16]).Append(proto.Null[int16]())
		} else {
			value, ok := val.(int16)
			if !ok {
				return errors.Errorf("%v is not the type int16", val)
			}
			bd.Data.(*proto.ColNullable[int16]).Append(proto.NewNullable[int16](value))
		}
		//bd.Data = new(proto.ColInt16).Nullable()
	case proto.ColumnTypeInt32:
		value, ok := val.(int32)
		if !ok {
			return errors.Errorf("%v is not the type int32", val)
		}
		bd.Data.(*proto.ColInt32).Append(value)
		//bd.Data = new(proto.ColInt32)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt32):
		if val == nil {
			bd.Data.(*proto.ColNullable[int32]).Append(proto.Null[int32]())
		} else {
			value, ok := val.(int32)
			if !ok {
				return errors.Errorf("%v is not the type int32", val)
			}
			bd.Data.(*proto.ColNullable[int32]).Append(proto.NewNullable[int32](value))
		}
		//bd.Data = new(proto.ColInt32).Nullable()
	case proto.ColumnTypeInt64:
		value, ok := val.(int64)
		if !ok {
			return errors.Errorf("%v is not the type int64", val)
		}
		bd.Data.(*proto.ColInt64).Append(value)
		//bd.Data = new(proto.ColInt64)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt64):
		if val == nil {
			bd.Data.(*proto.ColNullable[int64]).Append(proto.Null[int64]())
		} else {
			value, ok := val.(int64)
			if !ok {
				return errors.Errorf("%v is not the type int64", val)
			}
			bd.Data.(*proto.ColNullable[int64]).Append(proto.NewNullable[int64](value))
		}
		//bd.Data = new(proto.ColInt64).Nullable()
	case proto.ColumnTypeInt128:
		value, ok := val.(proto.Int128)
		if !ok {
			return errors.Errorf("%v is not the type proto.Int128", val)
		}
		bd.Data.(*proto.ColInt128).Append(value)
		//bd.Data = new(proto.ColInt128)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt128):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.Int128]).Append(proto.Null[proto.Int128]())
		} else {
			value, ok := val.(proto.Int128)
			if !ok {
				return errors.Errorf("%v is not the type int64", val)
			}
			bd.Data.(*proto.ColNullable[proto.Int128]).Append(proto.NewNullable[proto.Int128](value))
		}
		//bd.Data = new(proto.ColInt128).Nullable()
	case proto.ColumnTypeInt256:
		value, ok := val.(proto.Int256)
		if !ok {
			return errors.Errorf("%v is not the type proto.Int256", val)
		}
		bd.Data.(*proto.ColInt256).Append(value)
		//bd.Data = new(proto.ColInt256)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt256):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.Int256]).Append(proto.Null[proto.Int256]())
		} else {
			value, ok := val.(proto.Int256)
			if !ok {
				return errors.Errorf("%v is not the type int64", val)
			}
			bd.Data.(*proto.ColNullable[proto.Int256]).Append(proto.NewNullable[proto.Int256](value))
		}
		//bd.Data = new(proto.ColInt256).Nullable()
	case proto.ColumnTypeUInt8:
		value, ok := val.(uint8)
		if !ok {
			return errors.Errorf("%v is not the type uint8", val)
		}
		bd.Data.(*proto.ColUInt8).Append(value)
		//bd.Data = new(proto.ColUInt8)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt8):
		if val == nil {
			bd.Data.(*proto.ColNullable[uint8]).Append(proto.Null[uint8]())
		} else {
			value, ok := val.(uint8)
			if !ok {
				return errors.Errorf("%v is not the type uint8", val)
			}
			bd.Data.(*proto.ColNullable[uint8]).Append(proto.NewNullable[uint8](value))
		}
		//bd.Data = new(proto.ColUInt8).Nullable()
	case proto.ColumnTypeUInt16:
		value, ok := val.(uint16)
		if !ok {
			return errors.Errorf("%v is not the type uint16", val)
		}
		bd.Data.(*proto.ColUInt16).Append(value)
		//bd.Data = new(proto.ColUInt16)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt16):
		if val == nil {
			bd.Data.(*proto.ColNullable[uint16]).Append(proto.Null[uint16]())
		} else {
			value, ok := val.(uint16)
			if !ok {
				return errors.Errorf("%v is not the type uint16", val)
			}
			bd.Data.(*proto.ColNullable[uint16]).Append(proto.NewNullable[uint16](value))
		}
		//bd.Data = new(proto.ColUInt16).Nullable()
	case proto.ColumnTypeUInt32:
		value, ok := val.(uint32)
		if !ok {
			return errors.Errorf("%v is not the type uint32", val)
		}
		bd.Data.(*proto.ColUInt32).Append(value)
		//bd.Data = new(proto.ColUInt32)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt32):
		if val == nil {
			bd.Data.(*proto.ColNullable[uint32]).Append(proto.Null[uint32]())
		} else {
			value, ok := val.(uint32)
			if !ok {
				return errors.Errorf("%v is not the type uint32", val)
			}
			bd.Data.(*proto.ColNullable[uint32]).Append(proto.NewNullable[uint32](value))
		}
		//bd.Data = new(proto.ColUInt32).Nullable()
	case proto.ColumnTypeUInt64:
		value, ok := val.(uint64)
		if !ok {
			return errors.Errorf("%v is not the type uint64", val)
		}
		bd.Data.(*proto.ColUInt64).Append(value)
		//bd.Data = new(proto.ColUInt64)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt64):
		if val == nil {
			bd.Data.(*proto.ColNullable[uint64]).Append(proto.Null[uint64]())
		} else {
			value, ok := val.(uint64)
			if !ok {
				return errors.Errorf("%v is not the type uint64", val)
			}
			bd.Data.(*proto.ColNullable[uint64]).Append(proto.NewNullable[uint64](value))
		}
		//bd.Data = new(proto.ColUInt64).Nullable()
	case proto.ColumnTypeUInt128:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is not the type string", val)
		}
		uInt128, err := UInt128FromHex(value)
		if err != nil {
			return errors.Errorf("%v is not the type hexadecimal characters string", val)
		}
		bd.Data.(*proto.ColUInt128).Append(uInt128)
		//bd.Data = new(proto.ColUInt128)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt128):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.UInt128]).Append(proto.Null[proto.UInt128]())
		} else {
			value, ok := val.(string)
			if !ok {
				return errors.Errorf("%v is not the type string", val)
			}
			uInt128, err := UInt128FromHex(value)
			if err != nil {
				return errors.Errorf("%v is not the type hexadecimal characters string", val)
			}
			bd.Data.(*proto.ColNullable[proto.UInt128]).Append(proto.NewNullable[proto.UInt128](uInt128))
		}
		//bd.Data = new(proto.ColUInt128).Nullable()
	case proto.ColumnTypeUInt256:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is not the type string", val)
		}
		uInt256, err := UInt256FromHex(value)
		if err != nil {
			return errors.Errorf("%v is not the type hexadecimal characters string", val)
		}
		bd.Data.(*proto.ColUInt256).Append(uInt256)
		//bd.Data = new(proto.ColUInt256)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt256):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.UInt256]).Append(proto.Null[proto.UInt256]())
		} else {
			value, ok := val.(string)
			if !ok {
				return errors.Errorf("%v is not the type string", val)
			}
			uInt256, err := UInt256FromHex(value)
			if err != nil {
				return errors.Errorf("%v is not the type hexadecimal characters string", val)
			}
			bd.Data.(*proto.ColNullable[proto.UInt256]).Append(proto.NewNullable[proto.UInt256](uInt256))
		}
		//bd.Data = new(proto.ColUInt256).Nullable()
	case proto.ColumnTypeFloat32:
		value, ok := val.(float32)
		if !ok {
			return errors.Errorf("%v is not the type float32", val)
		}
		bd.Data.(*proto.ColFloat32).Append(value)
		//bd.Data = new(proto.ColFloat32)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat32):
		if val == nil {
			bd.Data.(*proto.ColNullable[float32]).Append(proto.Null[float32]())
		} else {
			value, ok := val.(float32)
			if !ok {
				return errors.Errorf("%v is not the type float32", val)
			}
			bd.Data.(*proto.ColNullable[float32]).Append(proto.NewNullable[float32](value))
		}
		//bd.Data = new(proto.ColFloat32).Nullable()
	case proto.ColumnTypeFloat64:
		value, ok := val.(float64)
		if !ok {
			return errors.Errorf("%v is not the type float64", val)
		}
		bd.Data.(*proto.ColFloat64).Append(value)
		//bd.Data = new(proto.ColFloat64)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat64):
		if val == nil {
			bd.Data.(*proto.ColNullable[float64]).Append(proto.Null[float64]())
		} else {
			value, ok := val.(float64)
			if !ok {
				return errors.Errorf("%v is not the type float64", val)
			}
			bd.Data.(*proto.ColNullable[float64]).Append(proto.NewNullable[float64](value))
		}
		//bd.Data = new(proto.ColFloat64).Nullable()
	case proto.ColumnTypeString:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is not the type string", val)
		}
		bd.Data.(*proto.ColStr).Append(value)
		//bd.Data = new(proto.ColStr)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeString):
		if val == nil {
			bd.Data.(*proto.ColNullable[string]).Append(proto.Null[string]())
		} else {
			value, ok := val.(string)
			if !ok {
				return errors.Errorf("%v is not the type string", val)
			}
			bd.Data.(*proto.ColNullable[string]).Append(proto.NewNullable[string](value))
		}
		//bd.Data = new(proto.ColStr).Nullable()
	case proto.ColumnTypeFixedString:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is not the type string", val)
		}
		bd.Data.(*proto.ColFixedStr).Append([]byte(value))
		//bd.Data = new(proto.ColFixedStr)
	case proto.ColumnTypeArray:
		value, ok := val.([]string)
		if !ok {
			return errors.Errorf("%v is not the type []string", val)
		}
		bd.Data.(*proto.ColArr[string]).Append(value)
		//bd.Data = new(proto.ColArr[string])
	case proto.ColumnTypeIPv4:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is not the type string", val)
		}
		ip, err := netip.ParseAddr(value)
		if err != nil {
			return err
		}
		ipV4 := proto.ToIPv4(ip)
		bd.Data.(*proto.ColIPv4).Append(ipV4)
		//bd.Data = new(proto.ColIPv4)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeIPv4):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.IPv4]).Append(proto.Null[proto.IPv4]())
		} else {
			value, ok := val.(string)
			if !ok {
				return errors.Errorf("%v is not the type string", val)
			}
			ip, err := netip.ParseAddr(value)
			if err != nil {
				return err
			}
			ipV4 := proto.ToIPv4(ip)
			bd.Data.(*proto.ColNullable[proto.IPv4]).Append(proto.NewNullable[proto.IPv4](ipV4))
		}
		//bd.Data = new(proto.ColIPv4).Nullable()
	case proto.ColumnTypeIPv6:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is not the type string", val)
		}
		ip, err := netip.ParseAddr(value)
		if err != nil {
			return err
		}
		ipV6 := proto.ToIPv6(ip)

		bd.Data.(*proto.ColIPv6).Append(ipV6)
		//bd.Data = new(proto.ColIPv6)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeIPv6):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.IPv6]).Append(proto.Null[proto.IPv6]())
		} else {
			value, ok := val.(string)
			if !ok {
				return errors.Errorf("%v is not the type string", val)
			}
			ip, err := netip.ParseAddr(value)
			if err != nil {
				return err
			}
			ipV6 := proto.ToIPv6(ip)
			bd.Data.(*proto.ColNullable[proto.IPv6]).Append(proto.NewNullable[proto.IPv6](ipV6))
		}
		//bd.Data = new(proto.ColIPv6).Nullable()
	case proto.ColumnTypeDateTime:
		value, ok := val.(time.Time)
		if !ok {
			return errors.Errorf("%v is not the type time.Time", val)
		}
		bd.Data.(*proto.ColDateTime).Append(value)
		//bd.Data = new(proto.ColDateTime)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDateTime):
		if val == nil {
			bd.Data.(*proto.ColNullable[time.Time]).Append(proto.Null[time.Time]())
		} else {
			value, ok := val.(time.Time)
			if !ok {
				return errors.Errorf("%v is not the type time.Time", val)
			}
			bd.Data.(*proto.ColNullable[time.Time]).Append(proto.NewNullable[time.Time](value))
		}
		//bd.Data = new(proto.ColDateTime).Nullable()
	case proto.ColumnTypeDateTime64:
		value, ok := val.(time.Time)
		if !ok {
			return errors.Errorf("%v is not the type time.Time", val)
		}
		bd.Data.(*proto.ColDateTime64).Append(value)
		//bd.Data = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMicro)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDateTime64):
		if val == nil {
			bd.Data.(*proto.ColNullable[time.Time]).Append(proto.Null[time.Time]())
		} else {
			value, ok := val.(time.Time)
			if !ok {
				return errors.Errorf("%v is not the type time.Time", val)
			}
			bd.Data.(*proto.ColNullable[time.Time]).Append(proto.NewNullable[time.Time](value))
		}
		//bd.Data = proto.NewColNullable[time.Time](new(proto.ColDateTime64).WithPrecision(proto.PrecisionMicro))
	case proto.ColumnTypeDate:
		value, ok := val.(time.Time)
		if !ok {
			return errors.Errorf("%v is not the type time.Time", val)
		}
		bd.Data.(*proto.ColDate).Append(value)
		//bd.Data = new(proto.ColDate)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDate):
		if val == nil {
			bd.Data.(*proto.ColNullable[time.Time]).Append(proto.Null[time.Time]())
		} else {
			value, ok := val.(time.Time)
			if !ok {
				return errors.Errorf("%v is not the type time.Time", val)
			}
			bd.Data.(*proto.ColNullable[time.Time]).Append(proto.NewNullable[time.Time](value))
		}

		//bd.Data = new(proto.ColDate).Nullable()
	case proto.ColumnTypeDate32:
		value, ok := val.(time.Time)
		if !ok {
			return errors.Errorf("%v is not the type time.Time", val)
		}
		bd.Data.(*proto.ColDate32).Append(value)
		//bd.Data = new(proto.ColDate32)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDate32):
		if val == nil {
			bd.Data.(*proto.ColNullable[time.Time]).Append(proto.Null[time.Time]())
		} else {
			value, ok := val.(time.Time)
			if !ok {
				return errors.Errorf("%v is not the type time.Time", val)
			}
			bd.Data.(*proto.ColNullable[time.Time]).Append(proto.NewNullable[time.Time](value))
		}

		//bd.Data = new(proto.ColDate32).Nullable()
	case proto.ColumnTypeUUID:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is not the type string", val)
		}
		uuidVal, err := uuid.Parse(value)
		if err != nil {
			return err
		}
		bd.Data.(*proto.ColUUID).Append(uuidVal)
		//bd.Data = new(proto.ColUUID)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUUID):
		if val == nil {
			bd.Data.(*proto.ColNullable[uuid.UUID]).Append(proto.Null[uuid.UUID]())
		} else {
			value, ok := val.(string)
			if !ok {
				return errors.Errorf("%v is not the type string", val)
			}
			uuidVal, err := uuid.Parse(value)
			if err != nil {
				return err
			}
			bd.Data.(*proto.ColNullable[uuid.UUID]).Append(proto.NewNullable[uuid.UUID](uuidVal))
		}
		//bd.Data = new(proto.ColUUID).Nullable()
	case proto.ColumnTypeEnum8:
		value, ok := val.(int8)
		if !ok {
			return errors.Errorf("%v is not the type int8", val)
		}
		vEnum8 := proto.Enum8(value)
		bd.Data.(*proto.ColEnum8).Append(vEnum8)
		//bd.Data = new(proto.ColEnum8)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeEnum8):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.Enum8]).Append(proto.Null[proto.Enum8]())
		} else {
			value, ok := val.(int8)
			if !ok {
				return errors.Errorf("%v is not the type int8", val)
			}
			vEnum8 := proto.Enum8(value)
			bd.Data.(*proto.ColNullable[proto.Enum8]).Append(proto.NewNullable[proto.Enum8](vEnum8))
		}
		//bd.Data = new(proto.ColEnum8).Nullable()
	case proto.ColumnTypeEnum16:
		value, ok := val.(int16)
		if !ok {
			return errors.Errorf("%v is not the type int16", val)
		}
		vEnum16 := proto.Enum16(value)
		bd.Data.(*proto.ColEnum16).Append(vEnum16)
		//bd.Data = new(proto.ColEnum16)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeEnum16):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.Enum16]).Append(proto.Null[proto.Enum16]())
		} else {
			value, ok := val.(int16)
			if !ok {
				return errors.Errorf("%v is not the type int16", val)
			}
			vEnum16 := proto.Enum16(value)
			bd.Data.(*proto.ColNullable[proto.Enum16]).Append(proto.NewNullable[proto.Enum16](vEnum16))
		}
		//bd.Data = new(proto.ColEnum16).Nullable()
	case proto.ColumnTypeMap:
		value, ok := val.(map[string]string)
		if !ok {
			return errors.Errorf("%v is not the type map[string]string", val)
		}
		bd.Data.(*proto.ColMap[string, string]).Append(value)
		//bd.Data = new(proto.ColMap[string, string])
	case proto.ColumnTypeBool:
		value, ok := val.(bool)
		if !ok {
			return errors.Errorf("%v is not the type string", val)
		}
		bd.Data.(*proto.ColBool).Append(value)
		//bd.Data = new(proto.ColBool)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeBool):
		if val == nil {
			bd.Data.(*proto.ColNullable[bool]).Append(proto.Null[bool]())
		} else {
			value, ok := val.(bool)
			if !ok {
				return errors.Errorf("%v is not the type bool", val)
			}
			bd.Data.(*proto.ColNullable[bool]).Append(proto.NewNullable[bool](value))
		}
		//bd.Data = new(proto.ColBool).Nullable()
	case proto.ColumnTypeDecimal32:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is need convert to string", val)
		}
		numValue, err := convertDecimalToNumber(value)
		if err != nil {
			return err
		}
		valDecimal32 := proto.Decimal32(numValue)
		bd.Data.(*proto.ColDecimal32).Append(valDecimal32)
		//bd.Data = new(proto.ColDecimal32)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal32):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.Decimal32]).Append(proto.Null[proto.Decimal32]())
		} else {
			value, ok := val.(string)
			if !ok {
				return errors.Errorf("%v is need convert to string", val)
			}
			numValue, err := convertDecimalToNumber(value)
			if err != nil {
				return err
			}
			valDecimal32 := proto.Decimal32(numValue)
			bd.Data.(*proto.ColNullable[proto.Decimal32]).Append(proto.NewNullable[proto.Decimal32](valDecimal32))
		}
		//bd.Data = new(proto.ColDecimal32).Nullable()
	case proto.ColumnTypeDecimal64:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is need convert to string", val)
		}
		numValue, err := convertDecimalToNumber(value)
		if err != nil {
			return err
		}
		valDecimal64 := proto.Decimal64(numValue)

		bd.Data.(*proto.ColDecimal64).Append(valDecimal64)
		//bd.Data = new(proto.ColDecimal64)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal64):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.Decimal64]).Append(proto.Null[proto.Decimal64]())
		} else {
			value, ok := val.(string)
			if !ok {
				return errors.Errorf("%v is need convert to string", val)
			}
			numValue, err := convertDecimalToNumber(value)
			if err != nil {
				return err
			}
			valDecimal64 := proto.Decimal64(numValue)
			bd.Data.(*proto.ColNullable[proto.Decimal64]).Append(proto.NewNullable[proto.Decimal64](valDecimal64))
		}
		//bd.Data = new(proto.ColDecimal64).Nullable()
	case proto.ColumnTypeDecimal128:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is not the type string", val)
		}
		uInt128, err := UInt128FromHex(value)
		if err != nil {
			return errors.Errorf("%v is not the type hexadecimal characters string", val)
		}
		bd.Data.(*proto.ColDecimal128).Append(proto.Decimal128(uInt128))
		//bd.Data = new(proto.ColDecimal128)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal128):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.Decimal128]).Append(proto.Null[proto.Decimal128]())
		} else {
			value, ok := val.(string)
			if !ok {
				return errors.Errorf("%v is not the type string", val)
			}
			uInt128, err := UInt128FromHex(value)
			if err != nil {
				return errors.Errorf("%v is not the type hexadecimal characters string", val)
			}
			bd.Data.(*proto.ColNullable[proto.Decimal128]).Append(proto.NewNullable[proto.Decimal128](proto.Decimal128(uInt128)))
		}

		//bd.Data = new(proto.ColDecimal128).Nullable()
	case proto.ColumnTypeDecimal256:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is not the type string", val)
		}
		uInt256, err := UInt256FromHex(value)
		if err != nil {
			return errors.Errorf("%v is not the type hexadecimal characters string", val)
		}
		bd.Data.(*proto.ColDecimal256).Append(proto.Decimal256(uInt256))
		//bd.Data = new(proto.ColDecimal256)
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal256):
		if val == nil {
			bd.Data.(*proto.ColNullable[proto.Decimal256]).Append(proto.Null[proto.Decimal256]())
		} else {
			value, ok := val.(string)
			if !ok {
				return errors.Errorf("%v is not the type string", val)
			}
			uInt256, err := UInt256FromHex(value)
			if err != nil {
				return errors.Errorf("%v is not the type hexadecimal characters string", val)
			}
			bd.Data.(*proto.ColNullable[proto.Decimal256]).Append(proto.NewNullable[proto.Decimal256](proto.Decimal256(uInt256)))
		}
		//bd.Data = new(proto.ColDecimal256).Nullable()
	case proto.ColumnTypePoint:
		value, ok := val.(string)
		if !ok {
			return errors.Errorf("%v is not the type string", val)
		}
		point, err := PointFromStr(value)
		if err != nil {
			return err
		}
		bd.Data.(*proto.ColPoint).Append(point)
		//bd.Data = new(proto.ColPoint)
	case proto.ColumnTypeInterval:
		value, ok := val.(proto.Interval)
		if !ok {
			return errors.Errorf("%v is not then type of proto.Interval", val)
		}
		bd.Data.(*proto.ColInterval).Append(value)
	}
	return nil
}
func (bd *TBufferData) Reset() {
	switch bd.ColumnType {
	case proto.ColumnTypeNothing:
		bd.Data.(*proto.ColNothing).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeNothing):
		bd.Data.(*proto.ColNullable[proto.Nothing]).Reset()
	case proto.ColumnTypeInt8:
		bd.Data.(*proto.ColInt8).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt8):
		bd.Data.(*proto.ColNullable[int8]).Reset()
	case proto.ColumnTypeInt16:
		bd.Data.(*proto.ColInt16).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt16):
		bd.Data.(*proto.ColNullable[int16]).Reset()
	case proto.ColumnTypeInt32:
		bd.Data.(*proto.ColInt32).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt32):
		bd.Data.(*proto.ColNullable[int32]).Reset()
	case proto.ColumnTypeInt64:
		bd.Data.(*proto.ColInt64).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt64):
		bd.Data.(*proto.ColNullable[int64]).Reset()
	case proto.ColumnTypeInt128:
		bd.Data.(*proto.ColInt128).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt128):
		bd.Data.(*proto.ColNullable[proto.Int128]).Reset()
	case proto.ColumnTypeInt256:
		bd.Data.(*proto.ColInt256).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt256):
		bd.Data.(*proto.ColNullable[proto.Int256]).Reset()
	case proto.ColumnTypeUInt8:
		bd.Data.(*proto.ColUInt8).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt8):
		bd.Data.(*proto.ColNullable[uint8]).Reset()
	case proto.ColumnTypeUInt16:
		bd.Data.(*proto.ColUInt16).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt16):
		bd.Data.(*proto.ColNullable[uint16]).Reset()
	case proto.ColumnTypeUInt32:
		bd.Data.(*proto.ColUInt32).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt32):
		bd.Data.(*proto.ColNullable[uint32]).Reset()
	case proto.ColumnTypeUInt64:
		bd.Data.(*proto.ColUInt64).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt64):
		bd.Data.(*proto.ColNullable[uint64]).Reset()
	case proto.ColumnTypeUInt128:
		bd.Data.(*proto.ColUInt128).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt128):
		bd.Data.(*proto.ColNullable[proto.UInt128]).Reset()
	case proto.ColumnTypeUInt256:
		bd.Data.(*proto.ColUInt256).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt256):
		bd.Data.(*proto.ColNullable[proto.UInt256]).Reset()
	case proto.ColumnTypeFloat32:
		bd.Data.(*proto.ColFloat32).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat32):
		bd.Data.(*proto.ColNullable[float32]).Reset()
	case proto.ColumnTypeFloat64:
		bd.Data.(*proto.ColFloat64).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat64):
		bd.Data.(*proto.ColNullable[float64]).Reset()
	case proto.ColumnTypeString:
		bd.Data.(*proto.ColStr).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeString):
		bd.Data.(*proto.ColNullable[string]).Reset()
	case proto.ColumnTypeFixedString:
		bd.Data.(*proto.ColFixedStr).Reset()
	case proto.ColumnTypeArray:
		bd.Data.(*proto.ColArr[string]).Reset()
	case proto.ColumnTypeIPv4:
		bd.Data.(*proto.ColIPv4).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeIPv4):
		bd.Data.(*proto.ColNullable[proto.IPv4]).Reset()
	case proto.ColumnTypeIPv6:
		bd.Data.(*proto.ColIPv6).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeIPv6):
		bd.Data.(*proto.ColNullable[proto.IPv6]).Reset()
	case proto.ColumnTypeDateTime:
		bd.Data.(*proto.ColDateTime).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDateTime):
		bd.Data.(*proto.ColNullable[time.Time]).Reset()
	case proto.ColumnTypeDateTime64:
		bd.Data.(*proto.ColDateTime64).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDateTime64):
		bd.Data.(*proto.ColNullable[time.Time]).Reset()
	case proto.ColumnTypeDate:
		bd.Data.(*proto.ColDate).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDate):
		bd.Data.(*proto.ColNullable[time.Time]).Reset()
	case proto.ColumnTypeDate32:
		bd.Data.(*proto.ColDate32).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDate32):
		bd.Data.(*proto.ColNullable[time.Time]).Reset()
	case proto.ColumnTypeUUID:
		bd.Data.(*proto.ColUUID).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeUUID):
		bd.Data.(*proto.ColNullable[uuid.UUID]).Reset()
	case proto.ColumnTypeEnum8:
		bd.Data.(*proto.ColEnum8).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeEnum8):
		bd.Data.(*proto.ColNullable[proto.Enum8]).Reset()
	case proto.ColumnTypeEnum16:
		bd.Data.(*proto.ColEnum16).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeEnum16):
		bd.Data.(*proto.ColNullable[proto.Enum16]).Reset()
	case proto.ColumnTypeMap:
		bd.Data.(*proto.ColMap[string, string]).Reset()
	case proto.ColumnTypeBool:
		bd.Data.(*proto.ColBool).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeBool):
		bd.Data.(*proto.ColNullable[bool]).Reset()
	case proto.ColumnTypeDecimal32:
		bd.Data.(*proto.ColDecimal32).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal32):
		bd.Data.(*proto.ColNullable[proto.Decimal32]).Reset()
	case proto.ColumnTypeDecimal64:
		bd.Data.(*proto.ColDecimal64).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal64):
		bd.Data.(*proto.ColNullable[proto.Decimal64]).Reset()
	case proto.ColumnTypeDecimal128:
		bd.Data.(*proto.ColDecimal128).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal128):
		bd.Data.(*proto.ColNullable[proto.Decimal128]).Reset()
	case proto.ColumnTypeDecimal256:
		bd.Data.(*proto.ColDecimal256).Reset()
	case proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal256):
		bd.Data.(*proto.ColNullable[proto.Decimal256]).Reset()
	case proto.ColumnTypePoint:
		bd.Data.(*proto.ColPoint).Reset()
	case proto.ColumnTypeInterval:
		bd.Data.(*proto.ColInterval).Reset()
	}
}
