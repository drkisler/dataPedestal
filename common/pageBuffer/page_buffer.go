package pageBuffer

import (
	"fmt"
	"strconv"
	"strings"
)

type PageBuffer struct {
	UserID     int32
	QueryParam string
	Total      int64
	Page       [][]int64
}

func NewPageBuffer(userID int32, queryParam string, pageSize int64, ids []int64) PageBuffer {
	var iLen = int64(len(ids))
	rowCount := iLen / pageSize
	if iLen%pageSize != 0 {
		rowCount++
	}

	// 创建二维数组
	matrix := make([][]int64, rowCount)

	// 填充二维数组
	row := 0
	col := int64(0)
	for i := int64(0); i < int64(len(ids)); i++ {
		if col == 0 {
			matrix[row] = make([]int64, 0, pageSize)
		}

		matrix[row] = append(matrix[row], ids[i])
		col++

		if col == pageSize {
			row++
			col = 0
		}
	}
	return PageBuffer{userID, queryParam, iLen, matrix}
}

func (pg *PageBuffer) GetPageIDs(pageIndex int64) (*string, error) {
	if pageIndex < 0 || pageIndex >= int64(len(pg.Page)) {
		return nil, fmt.Errorf("page index %d is out of range[0, %d]", pageIndex, len(pg.Page))
	}
	page := pg.Page[pageIndex]
	var sb strings.Builder

	for _, v := range page {
		sb.WriteString(strconv.Itoa(int(v)))
		sb.WriteString(",")
	}
	result := sb.String()
	return &result, nil
}
