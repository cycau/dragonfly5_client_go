// Copyright 2025 kg.sai. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rdbclient

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// Binary protocol constants (v4: slim binary format)
const (
	binProtoVersion byte   = 0x04
	binEndOfRows    uint32 = 0x00000000
	binTrailerError byte   = 0x01
)

// WireType constants for binary stream column values
const (
	WireNULL  byte = 0x00
	WireBOOL1 byte = 0x01
	WireBOOL0 byte = 0x02

	WireINT8     byte = 0x03
	WireINT16    byte = 0x04
	WireINT32    byte = 0x05
	WireINT64    byte = 0x06
	WireUINT8    byte = 0x07
	WireUINT16   byte = 0x08
	WireUINT32   byte = 0x09
	WireUINT64   byte = 0x0A
	WireFLOAT32  byte = 0x0B
	WireFLOAT64  byte = 0x0C
	WireDATETIME byte = 0x0D

	WireSTRING byte = 0x0E
	WireBYTES  byte = 0x0F
)

// ColumnMeta はカラムメタデータ
type ColumnMeta struct {
	Name     string `json:"name"`
	DBType   string `json:"dbType"`
	WireType byte   `json:"wireType"`
	Nullable bool   `json:"nullable"`
}

// QueryResponse は Query の結果
type queryResponse struct {
	Meta          []ColumnMeta `json:"meta,omitempty"`
	Rows          [][]any      `json:"rows"`
	TotalCount    int          `json:"totalCount"`
	ElapsedTimeUs int64        `json:"elapsedTimeUs"`
}

type Record struct {
	colMap *map[string]int
	data   *[]any
}

func (r *Record) Get(columnName string) any {
	idx, ok := (*r.colMap)[columnName]
	if !ok {
		panic(fmt.Sprintf("Column name %s not exist", columnName))
	}
	return (*r.data)[idx]
}

func (r *Record) GetByIdx(idx int) any {
	return (*r.data)[idx]
}

type Records struct {
	colMap        map[string]int
	Meta          []ColumnMeta
	Rows          []Record
	TotalCount    int
	ElapsedTimeUs int64
}

func (r *Records) Get(rowIndex int) *Record {
	if rowIndex < 0 || rowIndex >= len(r.Rows) {
		return nil
	}
	return &r.Rows[rowIndex]
}

func (r *Records) Size() int {
	return len(r.Rows)
}

// makeRecords converts the raw queryResponse into a Records struct with proper type restoration based on the column metadata.
func parseQueryResult(resp *responseResult) (*Records, *HaveError) {
	var result queryResponse

	if strings.Contains(resp.ContentType, "octet") {
		if err := parseBinaryQueryResult(resp.Body, &result); err != nil {
			return nil, err
		}
	} else if strings.Contains(resp.ContentType, "json") {
		if err := parseJsonQueryResult(resp.Body, &result); err != nil {
			return nil, err
		}
	} else {
		return nil, &HaveError{
			ErrCode: "UnsupportedContentType",
			Message: fmt.Sprintf("unsupported content type: %s", resp.ContentType),
		}
	}

	return makeRecords(&result)
}

// parseJsonQueryResult decodes the JSON response body and restores types based on the wire type metadata.
func parseJsonQueryResult(data []byte, result *queryResponse) *HaveError {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(result); err != nil {
		return &HaveError{
			ErrCode: "ResponseBodyJSONDecodeError",
			Message: err.Error(),
		}
	}
	restoreTypes := make([]func(val *any) error, len(result.Meta))
	for i, col := range result.Meta {
		restoreTypes[i] = restoreJsonTypeFn(col.WireType)
	}

	for i := range result.Rows {
		row := &result.Rows[i]
		for j := range *row {
			if (*row)[j] == nil {
				continue
			}
			if err := restoreTypes[j](&(*row)[j]); err != nil {
				return &HaveError{
					ErrCode: "RestoreJsonTypeError",
					Message: err.Error(),
				}
			}
		}
	}

	return nil
}

// restoreJsonTypeFn returns a function that converts a raw JSON value to the appropriate Go type based on the wire type.
func restoreJsonTypeFn(wireType byte) func(pVal *any) error {
	switch wireType {
	case WireNULL:
		return func(val *any) error {
			return nil
		}
	case WireBOOL0, WireBOOL1:
		return func(pVal *any) error {
			val := *pVal
			if v, ok := (val).(bool); ok {
				*pVal = v
				return nil
			}
			if v, ok := (val).(string); ok {
				switch v {
				case "true", "TRUE", "1":
					*pVal = true
					return nil
				case "false", "FALSE", "0":
					*pVal = false
					return nil
				}
			}
			return fmt.Errorf("Invalid bool value: %v", val)
		}
	case WireINT8, WireINT16, WireINT32:
		return func(pVal *any) error {
			val := *pVal
			if v, ok := val.(json.Number); ok {
				n, err := v.Int64()
				if err != nil {
					return fmt.Errorf("Invalid int32 number: %v", err)
				}
				*pVal = int32(n)
				return nil
			}
			if v, ok := (val).(float64); ok {
				*pVal = int32(v)
				return nil
			}
			if v, ok := (val).(string); ok {
				var intVal int32
				_, err := fmt.Sscanf(v, "%d", &intVal)
				if err != nil {
					return fmt.Errorf("Invalid int32 string: %v", err)
				}
				*pVal = intVal
				return nil
			}
			s := fmt.Sprint(val)
			var intVal int32
			_, err := fmt.Sscanf(s, "%d", &intVal)
			if err != nil {
				return fmt.Errorf("Invalid int32 string: %v", err)
			}
			*pVal = intVal
			return nil
		}
	case WireINT64:
		return func(pVal *any) error {
			val := *pVal
			if v, ok := val.(json.Number); ok {
				n, err := v.Int64()
				if err != nil {
					return fmt.Errorf("Invalid int64 number: %v", err)
				}
				*pVal = n
				return nil
			}
			if v, ok := (val).(float64); ok {
				*pVal = int64(v)
				return nil
			}
			if v, ok := (val).(string); ok {
				var intVal int64
				_, err := fmt.Sscanf(v, "%d", &intVal)
				if err != nil {
					return fmt.Errorf("Invalid int64 string: %v", err)
				}
				*pVal = intVal
				return nil
			}
			s := fmt.Sprint(val)
			var intVal int64
			_, err := fmt.Sscanf(s, "%d", &intVal)
			if err != nil {
				return fmt.Errorf("Invalid int64 string: %v", err)
			}
			*pVal = intVal
			return nil
		}
	case WireUINT8, WireUINT16, WireUINT32:
		return func(pVal *any) error {
			val := *pVal
			if v, ok := val.(json.Number); ok {
				n, err := v.Int64()
				if err != nil {
					return fmt.Errorf("Invalid uint32 number: %v", err)
				}
				*pVal = uint32(n)
				return nil
			}
			if v, ok := (val).(float64); ok {
				*pVal = uint32(v)
				return nil
			}
			if v, ok := (val).(string); ok {
				var intVal uint32
				_, err := fmt.Sscanf(v, "%d", &intVal)
				if err != nil {
					return fmt.Errorf("Invalid uint32 string: %v", err)
				}
				*pVal = intVal
				return nil
			}
			s := fmt.Sprint(val)
			var intVal uint32
			_, err := fmt.Sscanf(s, "%d", &intVal)
			if err != nil {
				return fmt.Errorf("Invalid uint32 string: %v", err)
			}
			*pVal = intVal
			return nil
		}
	case WireUINT64:
		return func(pVal *any) error {
			val := *pVal
			if v, ok := val.(json.Number); ok {
				n, err := strconv.ParseUint(v.String(), 10, 64)
				if err != nil {
					return fmt.Errorf("Invalid uint64 number: %v", err)
				}
				*pVal = n
				return nil
			}
			if v, ok := (val).(float64); ok {
				*pVal = uint64(v)
				return nil
			}
			if v, ok := (val).(string); ok {
				var intVal uint64
				_, err := fmt.Sscanf(v, "%d", &intVal)
				if err != nil {
					return fmt.Errorf("Invalid uint64 string: %v", err)
				}
				*pVal = intVal
				return nil
			}
			s := fmt.Sprint(val)
			var intVal uint64
			_, err := fmt.Sscanf(s, "%d", &intVal)
			if err != nil {
				return fmt.Errorf("Invalid uint64 string: %v", err)
			}
			*pVal = intVal
			return nil
		}
	case WireFLOAT32:
		return func(pVal *any) error {
			val := *pVal
			if v, ok := val.(json.Number); ok {
				n, err := v.Float64()
				if err != nil {
					return fmt.Errorf("Invalid float32 number: %v", err)
				}
				*pVal = float32(n)
				return nil
			}
			if v, ok := (val).(float64); ok {
				*pVal = float32(v)
				return nil
			}
			if v, ok := (val).(string); ok {
				var floatVal float32
				_, err := fmt.Sscanf(v, "%f", &floatVal)
				if err != nil {
					return fmt.Errorf("Invalid float32 string: %v", err)
				}
				*pVal = floatVal
				return nil
			}
			s := fmt.Sprint(val)
			var floatVal float32
			_, err := fmt.Sscanf(s, "%f", &floatVal)
			if err != nil {
				return fmt.Errorf("Invalid float32 string: %v", err)
			}
			*pVal = floatVal
			return nil
		}
	case WireFLOAT64:
		return func(pVal *any) error {
			val := *pVal
			if v, ok := val.(json.Number); ok {
				n, err := v.Float64()
				if err != nil {
					return fmt.Errorf("Invalid float64 number: %v", err)
				}
				*pVal = n
				return nil
			}
			if v, ok := (val).(float64); ok {
				*pVal = v
				return nil
			}
			if v, ok := (val).(string); ok {
				var floatVal float64
				_, err := fmt.Sscanf(v, "%f", &floatVal)
				if err != nil {
					return fmt.Errorf("Invalid float64 string: %v", err)
				}
				*pVal = floatVal
				return nil
			}
			s := fmt.Sprint(val)
			var floatVal float64
			_, err := fmt.Sscanf(s, "%f", &floatVal)
			if err != nil {
				return fmt.Errorf("Invalid float64 string: %v", err)
			}
			*pVal = floatVal
			return nil
		}
	case WireDATETIME:
		return func(pVal *any) error {
			val := *pVal
			if v, ok := (val).(time.Time); ok {
				*pVal = v
				return nil
			}
			if v, ok := (val).(string); ok {
				t, err := time.Parse(time.RFC3339Nano, v)
				if err != nil {
					return fmt.Errorf("Invalid timestamp string: %v", err)
				}
				*pVal = t
				return nil
			}
			if v, ok := val.(json.Number); ok {
				n, err := v.Int64()
				if err != nil {
					return fmt.Errorf("Invalid datetime number: %v", err)
				}
				*pVal = time.Unix(0, n)
				return nil
			}
			if v, ok := (val).(float64); ok {
				*pVal = time.Unix(0, int64(v))
				return nil
			}
			return fmt.Errorf("Invalid timestamp value: %v", val)
		}
	case WireSTRING:
		return func(pVal *any) error {
			val := *pVal
			if v, ok := (val).(string); ok {
				*pVal = v
				return nil
			}
			if v, ok := (val).([]byte); ok {
				*pVal = string(v)
				return nil
			}
			return fmt.Errorf("Invalid string value: %v", val)
		}
	case WireBYTES:
		return func(pVal *any) error {
			val := *pVal
			if _, ok := (val).([]byte); ok {
				return nil
			}
			if v, ok := (val).(string); ok {
				decoded, err := base64.StdEncoding.DecodeString(v)
				if err != nil {
					return fmt.Errorf("Invalid base64 string: %v", err)
				}
				*pVal = decoded
				return nil
			}
			return fmt.Errorf("Invalid string value: %v", val)
		}
	default:
		return func(val *any) error {
			return fmt.Errorf("unknown wire type: 0x%02x", wireType)
		}
	}
}

// parseBinaryQueryResult decodes the binary octet-stream protocol (v4).
// Protocol format (matches slimResponse.go):
//
//	Section 1 (Meta):   [version:1] [metaCount:2] per-column{[nameLen:2] name [dbTypeLen:2] dbType [nullable:1]}
//	Section 2 (Rows):   per-row{[rowLen:4] per-column{[wireType] [valueLen:4(only when variable length)] value}}
//	Section 3 (Trailer): [endMarker:4=0] [type:1] [totalCount:8] [elapsedUs:8]
//	                 or  [endMarker:4=0] [type:1=err] [msgLen:2] msg
func parseBinaryQueryResult(data []byte, result *queryResponse) *HaveError {
	r := bytes.NewReader(data)

	// === Section 1: Header + Column Metadata (binary) ===
	version, err := r.ReadByte()
	if err != nil || version != binProtoVersion {
		return &HaveError{ErrCode: "BinaryProtocolError", Message: "invalid protocol version"}
	}

	var metaCount uint16
	if err := binary.Read(r, binary.BigEndian, &metaCount); err != nil {
		return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read meta count"}
	}

	meta := make([]ColumnMeta, metaCount)
	for i := uint16(0); i < metaCount; i++ {
		var nameLen, dbTypeLen uint16
		if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
			return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read column name length"}
		}
		nameBytes := make([]byte, nameLen)
		if _, err := r.Read(nameBytes); err != nil {
			return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read column name"}
		}
		if err := binary.Read(r, binary.BigEndian, &dbTypeLen); err != nil {
			return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read dbType length"}
		}
		dbTypeBytes := make([]byte, dbTypeLen)
		if _, err := r.Read(dbTypeBytes); err != nil {
			return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read dbType"}
		}
		nullable, err := r.ReadByte()
		if err != nil {
			return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read nullable"}
		}
		meta[i] = ColumnMeta{
			Name:     string(nameBytes),
			DBType:   string(dbTypeBytes),
			Nullable: nullable != 0,
		}
	}

	// === Section 2: Row Data (wire type based) ===
	var rows [][]any
	for {
		var rowLen uint32
		if err := binary.Read(r, binary.BigEndian, &rowLen); err != nil {
			return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read row length"}
		}
		if rowLen == binEndOfRows { // end-of-rows marker
			break
		}

		rowBytes := make([]byte, rowLen)
		if _, err := r.Read(rowBytes); err != nil {
			return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read row data"}
		}

		row, err := parseBinaryRow(rowBytes, int(metaCount))
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}

	// === Section 3: Trailer ===
	trailerType, err := r.ReadByte()
	if err != nil {
		return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read trailer type"}
	}
	if trailerType == binTrailerError {
		var msgLen uint16
		if err := binary.Read(r, binary.BigEndian, &msgLen); err != nil {
			return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read error message length"}
		}
		msgBytes := make([]byte, msgLen)
		if _, err := r.Read(msgBytes); err != nil {
			return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read error message"}
		}
		return &HaveError{ErrCode: "ServerStreamError", Message: string(msgBytes)}
	}

	// success trailer
	var totalCount int64
	var elapsedUs int64
	if err := binary.Read(r, binary.BigEndian, &totalCount); err != nil {
		return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read totalCount"}
	}
	if err := binary.Read(r, binary.BigEndian, &elapsedUs); err != nil {
		return &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read elapsedTimeUs"}
	}

	if rows == nil {
		rows = [][]any{}
	}

	result.Meta = meta
	result.Rows = rows
	result.TotalCount = int(totalCount)
	result.ElapsedTimeUs = elapsedUs

	return nil
}

// parseBinaryRow decodes a single row from wire-format bytes.
// Format: per-column{[wireType] [valueLen:4(only STRING/BYTES)] value}
func parseBinaryRow(rowBytes []byte, columnCount int) ([]any, *HaveError) {
	r := bytes.NewReader(rowBytes)
	row := make([]any, columnCount)
	for i := range row {
		wireType, readErr := r.ReadByte()
		if readErr != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read wire type"}
		}
		val, haveErr := parseBinaryColumn(r, wireType)
		if haveErr != nil {
			return nil, haveErr
		}
		row[i] = val
	}
	return row, nil
}

// parseBinaryColumn decodes a single column value based on the wire type.
func parseBinaryColumn(r *bytes.Reader, wireType byte) (any, *HaveError) {
	switch wireType {
	case WireNULL:
		return nil, nil
	case WireBOOL0:
		return false, nil
	case WireBOOL1:
		return true, nil
	case WireINT8:
		v, err := r.ReadByte()
		if err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read INT8"}
		}
		return int32(int8(v)), nil
	case WireINT16:
		var v int16
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read INT16"}
		}
		return int32(v), nil
	case WireINT32:
		var v int32
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read INT32"}
		}
		return v, nil
	case WireINT64:
		var v int64
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read INT64"}
		}
		return v, nil
	case WireUINT8:
		v, err := r.ReadByte()
		if err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read UINT8"}
		}
		return uint32(v), nil
	case WireUINT16:
		var v uint16
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read UINT16"}
		}
		return uint32(v), nil
	case WireUINT32:
		var v uint32
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read UINT32"}
		}
		return v, nil
	case WireUINT64:
		var v uint64
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read UINT64"}
		}
		return v, nil
	case WireFLOAT32:
		var v float32
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read FLOAT32"}
		}
		return v, nil
	case WireFLOAT64:
		var v float64
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read FLOAT64"}
		}
		return v, nil
	case WireDATETIME:
		var valueLen uint8
		if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read DATETIME"}
		}
		b := make([]byte, valueLen)
		if _, err := r.Read(b); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read DATETIME string"}
		}
		t, err := time.Parse(time.RFC3339Nano, string(b))
		if err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: fmt.Sprintf("Invalid DATETIME string: %v", err)}
		}
		return t, nil
	case WireSTRING:
		var valueLen uint32
		if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read STRING length"}
		}
		b := make([]byte, valueLen)
		if _, err := r.Read(b); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read STRING"}
		}
		return string(b), nil
	case WireBYTES:
		var valueLen uint32
		if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read BYTES length"}
		}
		b := make([]byte, valueLen)
		if _, err := r.Read(b); err != nil {
			return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: "failed to read BYTES"}
		}
		return b, nil
	default:
		return nil, &HaveError{ErrCode: "BinaryProtocolError", Message: fmt.Sprintf("unknown wire type: 0x%02x", wireType)}
	}
}

// makeRecords converts the raw queryResponse into a Records struct with proper type restoration based on the column metadata.
func makeRecords(result *queryResponse) (*Records, *HaveError) {
	if result == nil {
		return nil, &HaveError{ErrCode: "InvalidArgument", Message: "result is nil"}
	}
	records := Records{
		colMap:        make(map[string]int),
		Meta:          result.Meta,
		Rows:          make([]Record, len(result.Rows)),
		TotalCount:    result.TotalCount,
		ElapsedTimeUs: result.ElapsedTimeUs,
	}
	restoreDbTypes := make([]func(val *any) error, len(result.Meta))
	for i, col := range result.Meta {
		records.colMap[col.Name] = i
		restoreDbTypes[i] = restoreDbTypeFn(col.DBType)
	}
	for i := range records.Rows {
		records.Rows[i].colMap = &records.colMap
		records.Rows[i].data = &result.Rows[i]
		for j := range result.Rows[i] {
			if result.Rows[i][j] == nil {
				continue
			}
			if err := restoreDbTypes[j](&result.Rows[i][j]); err != nil {
				return nil, &HaveError{ErrCode: "InvalidType", Message: fmt.Sprintf("Failed to restore RowIndex %d, ColumnName %s, DB type: %s, value: %v, error: %v", i, result.Meta[j].Name, result.Meta[j].DBType, result.Rows[i][j], err.Error())}
			}
		}
	}
	return &records, nil
}

// restoreDbTypeFn returns a function that converts a raw JSON value to the appropriate Go type based on the DB type metadata.
func restoreDbTypeFn(dbType string) func(pVal *any) error {
	dbType = strings.ToUpper(dbType)

	switch dbType {
	case "BOOL", "BOOLEAN":
		return func(pVal *any) error {
			val := *pVal
			if _, ok := (val).(bool); ok {
				return nil
			}
			if v, ok := (val).(string); ok {
				switch strings.ToLower(v) {
				case "true", "1":
					*pVal = true
					return nil
				case "false", "0":
					*pVal = false
					return nil
				}
			}
			return fmt.Errorf("Invalid bool value: %v", val)
		}
	case "INT", "INT2", "INT4", "TINYINT", "SMALLINT", "MEDIUMINT":
		return func(pVal *any) error {
			val := *pVal
			if _, ok := (val).(int32); ok {
				return nil
			}
			if v, ok := (val).(int64); ok {
				*pVal = int32(v)
				return nil
			}
			if v, ok := (val).(float64); ok {
				*pVal = int32(v)
				return nil
			}
			if v, ok := (val).(string); ok {
				var intVal int32
				_, err := fmt.Sscanf(v, "%d", &intVal)
				if err != nil {
					return fmt.Errorf("Invalid int32 string: %v", err)
				}
				*pVal = intVal
				return nil
			}
			s := fmt.Sprint(val)
			var intVal int32
			_, err := fmt.Sscanf(s, "%d", &intVal)
			if err != nil {
				return fmt.Errorf("Invalid int32 string: %v", err)
			}
			*pVal = intVal
			return nil
		}
	case "LONG", "INT8", "BIGINT":
		return func(pVal *any) error {
			val := *pVal
			if _, ok := (val).(int64); ok {
				return nil
			}
			if v, ok := (val).(int32); ok {
				*pVal = int64(v)
				return nil
			}
			if v, ok := (val).(float64); ok {
				*pVal = int64(v)
				return nil
			}
			if v, ok := (val).(string); ok {
				var intVal int64
				_, err := fmt.Sscanf(v, "%d", &intVal)
				if err != nil {
					return fmt.Errorf("Invalid int64 string: %v", err)
				}
				*pVal = intVal
				return nil
			}
			s := fmt.Sprint(val)
			var intVal int64
			_, err := fmt.Sscanf(s, "%d", &intVal)
			if err != nil {
				return fmt.Errorf("Invalid int64 string: %v", err)
			}
			*pVal = intVal
			return nil
		}
	case "FLOAT", "FLOAT4":
		return func(pVal *any) error {
			val := *pVal
			if _, ok := (val).(float32); ok {
				return nil
			}
			if v, ok := (val).(float64); ok {
				*pVal = float32(v)
				return nil
			}
			if v, ok := (val).(string); ok {
				var floatVal float32
				_, err := fmt.Sscanf(v, "%f", &floatVal)
				if err != nil {
					return fmt.Errorf("Invalid float32 string: %v", err)
				}
				*pVal = floatVal
				return nil
			}
			s := fmt.Sprint(val)
			var floatVal float32
			_, err := fmt.Sscanf(s, "%f", &floatVal)
			if err != nil {
				return fmt.Errorf("Invalid float32 string: %v", err)
			}
			*pVal = floatVal
			return nil
		}
	case "DOUBLE", "FLOAT8":
		return func(pVal *any) error {
			val := *pVal
			if _, ok := (val).(float64); ok {
				return nil
			}
			if v, ok := (val).(float32); ok {
				*pVal = float64(v)
				return nil
			}
			if v, ok := (val).(string); ok {
				var floatVal float64
				_, err := fmt.Sscanf(v, "%f", &floatVal)
				if err != nil {
					return fmt.Errorf("Invalid float64 string: %v", err)
				}
				*pVal = floatVal
				return nil
			}
			s := fmt.Sprint(val)
			var floatVal float64
			_, err := fmt.Sscanf(s, "%f", &floatVal)
			if err != nil {
				return fmt.Errorf("Invalid float64 string: %v", err)
			}
			*pVal = floatVal
			return nil
		}
	case "NUMERIC", "DECIMAL":
		return func(pVal *any) error {
			val := *pVal
			if _, ok := (val).(decimal.Decimal); ok {
				return nil
			}
			if v, ok := (val).(string); ok {
				d, err := decimal.NewFromString(v)
				if err != nil {
					return fmt.Errorf("Invalid decimal string: %v", err)
				}
				*pVal = d
				return nil
			}
			if v, ok := (val).([]byte); ok {
				d, err := decimal.NewFromString(string(v))
				if err != nil {
					return fmt.Errorf("Invalid decimal string: %v", err)
				}
				*pVal = d
				return nil
			}
			return fmt.Errorf("Invalid decimal value: %v", val)
		}
	case "VARCHAR", "CHAR", "BPCHAR", "CHARACTER", "TEXT":
		return func(pVal *any) error {
			val := *pVal
			if _, ok := (val).(string); ok {
				return nil
			}
			if v, ok := (val).([]byte); ok {
				*pVal = string(v)
				return nil
			}
			return fmt.Errorf("Invalid varchar value: %v", val)
		}
	case "TIMESTAMP", "TIMESTAMPTZ", "DATE", "DATETIME":
		return func(pVal *any) error {
			val := *pVal
			if _, ok := (val).(time.Time); ok {
				return nil
			}
			if v, ok := (val).(int64); ok {
				*pVal = time.Unix(0, v)
				return nil
			}
			if v, ok := (val).(string); ok {
				t, err := time.Parse(time.RFC3339Nano, v)
				if err != nil {
					return fmt.Errorf("Invalid timestamp string: %v", err)
				}
				*pVal = t
				return nil
			}
			return fmt.Errorf("Invalid timestamp value: %v", val)
		}
	case "BYTEA":
		return func(pVal *any) error {
			val := *pVal
			if _, ok := (val).([]byte); ok {
				return nil
			}
			if v, ok := (val).(string); ok {
				data, err := base64.StdEncoding.DecodeString(v)
				if err != nil {
					return fmt.Errorf("Invalid bytea string: %v", err)
				}
				*pVal = data
				return nil
			}
			return fmt.Errorf("Invalid bytea value: %v", val)
		}
	default: // As is, do nothing
		// UUID string like "123e4567-e89b-12d3-a456-426614174000"
		// INTERVAL string like "1 year 2 months 3 days 4 hours 5 minutes 6 seconds"
		// BIT string like "10101010"
		// VARBIT string like "101010"
		// TIMETZ, 1266: string like "12:34:56+09"
		// Array  string like "{1,2,3}"
		// ... etc
		return func(pVal *any) error {
			return nil
		}
	}
}
