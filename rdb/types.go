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
	"fmt"
	"time"
)

type NodeEntry struct {
	BaseURL   string `yaml:"baseUrl"`
	SecretKey string `yaml:"secretKey"`
}

type ValueType string

const (
	ValueType_NULL     ValueType = "NULL"
	ValueType_BOOL     ValueType = "BOOL"
	ValueType_INT      ValueType = "INT"
	ValueType_LONG     ValueType = "LONG"
	ValueType_FLOAT    ValueType = "FLOAT"
	ValueType_DOUBLE   ValueType = "DOUBLE"
	ValueType_DECIMAL  ValueType = "DECIMAL"
	ValueType_DATE     ValueType = "DATE"
	ValueType_DATETIME ValueType = "DATETIME"
	ValueType_STRING   ValueType = "STRING"
	ValueType_BYTES    ValueType = "BYTES"
	ValueType_AS_IS    ValueType = "AS_IS"
)

// ParamValue はクエリ/実行のパラメータ
type paramValue struct {
	Value any       `json:"value,omitempty"`
	Type  ValueType `json:"type"`
}

type Params struct {
	data []paramValue
}

func (p *Params) Add(val any, valType ValueType) *Params {
	if val == nil {
		p.data = append(p.data, paramValue{
			Value: nil,
			Type:  ValueType_NULL,
		})
		return p
	}
	switch valType {
	case ValueType_BYTES: // json.Marshal will handle it to string with base64 encoding
	case ValueType_DECIMAL:
		p.data = append(p.data, paramValue{
			Value: fmt.Sprintf("%v", val),
			Type:  ValueType_STRING,
		})
		return p
	}

	p.data = append(p.data, paramValue{
		Value: val,
		Type:  valType,
	})
	return p
}

func NewParams() *Params {
	return &Params{
		data: []paramValue{},
	}
}

// QueryOptions は Query のオプション
type QueryOptions struct {
	OffsetRows int // 開始行
	LimitRows  int // 最大行数
	TimeoutSec int // タイムアウト秒
}

// ExecuteResult は Execute の結果
type ExecuteResult struct {
	EffectedRows  int64 `json:"effectedRows"`
	ElapsedTimeUs int64 `json:"elapsedTimeUs"`
}

type TxInfo struct {
	TxId      string    `json:"txId"`
	NodeID    string    `json:"nodeId"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// IsolationLevel はトランザクション分離レベル
type IsolationLevel string

const (
	Isolation_ReadUncommitted IsolationLevel = "READ_UNCOMMITTED"
	Isolation_ReadCommitted   IsolationLevel = "READ_COMMITTED"
	Isolation_RepeatableRead  IsolationLevel = "REPEATABLE_READ"
	Isolation_Serializable    IsolationLevel = "SERIALIZABLE"
)
