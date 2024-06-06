package common

import (
	"fmt"
	"strconv"
	"strings"
)

type PageBuffer struct {
	UserID     int32
	QueryParam string
	Total      int32
	Page       [][]int32
}

func NewPageBuffer(userID int32, queryParam string, pageSize int32, ids []int32) PageBuffer {
	var iLen = int32(len(ids))
	rowCount := iLen / pageSize
	if iLen%pageSize != 0 {
		rowCount++
	}

	// 创建二维数组
	matrix := make([][]int32, rowCount)

	// 填充二维数组
	row := 0
	col := int32(0)
	for i := 0; i < len(ids); i++ {
		if col == 0 {
			matrix[row] = make([]int32, 0, pageSize)
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

func (pg *PageBuffer) GetPageIDs(pageIndex int32) (*string, error) {
	if pageIndex < 0 || pageIndex >= int32(len(pg.Page)) {
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
