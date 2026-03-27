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
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	gonanoid "github.com/matoous/go-nanoid"
	"gopkg.in/yaml.v3"
)

type endpointType int

const (
	ep_QUERY endpointType = iota
	ep_EXECUTE
	ep_TX_BEGIN
	ep_TX_QUERY
	ep_TX_EXECUTE
	ep_TX_COMMIT
	ep_TX_ROLLBACK
	ep_OTHER
)

func getEndpointPath(ep endpointType) string {
	switch ep {
	case ep_QUERY:
		return "/query"
	case ep_EXECUTE:
		return "/execute"
	case ep_TX_BEGIN:
		return "/tx/begin"
	case ep_TX_QUERY:
		return "/tx/query"
	case ep_TX_EXECUTE:
		return "/tx/execute"
	case ep_TX_COMMIT:
		return "/tx/commit"
	case ep_TX_ROLLBACK:
		return "/tx/rollback"
	default:
		return "/other"
	}
}

const hEADER_SECRET_KEY = "_cy_SecretKey"
const hEADER_DB_NAME = "_cy_DbName"
const hEADER_TX_ID = "_cy_TxID"
const hEADER_REDIRECT_COUNT = "_cy_RdCount"
const hEADER_TIMEOUT_SEC = "_cy_TimeoutSec"

// clientConfig は config.yaml の構造
type clientConfig struct {
	MaxConcurrency   int         `yaml:"maxConcurrency"`
	DefaultSecretKey string      `yaml:"defaultSecretKey"`
	DefaultDatabase  string      `yaml:"defaultDatabase"`
	ClusterNodes     []NodeEntry `yaml:"clusterNodes"`
}

// nodeHealth は /healz レスポンス（サーバーと互換）
type nodeInfo struct {
	BaseURL      string           `yaml:"baseUrl"`
	SecretKey    string           `yaml:"secretKey"`
	NodeID       string           `json:"nodeId"`
	Status       string           `json:"status"`
	MaxHttpQueue int              `json:"maxHttpQueue"`
	CheckTime    time.Time        `json:"checkTime"`
	Datasources  []datasourceInfo `json:"datasources"`
	NextCheckAt  time.Time        `json:"-"`
	Mu           sync.RWMutex     `json:"-"`
}

type datasourceInfo struct {
	DatasourceID  string `json:"datasourceId"`
	DatabaseName  string `json:"databaseName"`
	Active        bool   `json:"active"`
	MaxConns      int    `json:"maxConns"`
	MaxWriteConns int    `json:"maxWriteConns"`
	MinWriteConns int    `json:"minWriteConns"`
}

var dEFAULT_DATABASE = ""

func Init(nodes []NodeEntry, defDatabase string, maxConcurrency int) error {
	if len(nodes) < 1 {
		return fmt.Errorf("No cluster nodes configured.")
	}

	maxConcurrency = max(10, maxConcurrency)

	log.Println("### [Init] maxConcurrency:", maxConcurrency)
	log.Println("### [Init] entries:", nodes)

	defaultDatabase, err := executor.Init(nodes, maxConcurrency, len(nodes))
	if defaultDatabase == "" {
		return fmt.Errorf("Failed to get datasource info from cluster nodes.")
	}
	dEFAULT_DATABASE = defDatabase
	if dEFAULT_DATABASE == "" {
		dEFAULT_DATABASE = defaultDatabase
	}

	log.Println("### [Init] defaultDatabase:", dEFAULT_DATABASE)
	return err
}

func InitWithYamlFile(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	var config clientConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	// defaultSecretKey をノードに適用
	for i := range config.ClusterNodes {
		if config.ClusterNodes[i].SecretKey == "" {
			config.ClusterNodes[i].SecretKey = config.DefaultSecretKey
		}
	}

	return Init(config.ClusterNodes, config.DefaultDatabase, config.MaxConcurrency)
}

/**************************************************
* DsClient
**************************************************/
const ALPHA_NUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

type Client struct {
	dbName   string
	executor *switcher
	traceId  string
}

// Get は databaseName 用のクライアントを返す。空の場合は defaultDatabase を使用する
func GetDefault() *Client {
	return Get("", "")
}
func Get(databaseName string, traceId string) *Client {
	if databaseName == "" {
		databaseName = dEFAULT_DATABASE
	}
	if traceId == "" {
		traceId, _ = gonanoid.Generate(ALPHA_NUM, 10)
		traceId = "d5" + traceId
	}

	return &Client{
		dbName:   databaseName,
		executor: executor,
		traceId:  traceId,
	}
}

func (c *Client) Query(ctx context.Context, sql string, params *Params, opts *QueryOptions) (*Records, *HaveError) {

	headers := map[string]string{
		hEADER_DB_NAME: c.dbName,
	}
	paramValues := []paramValue{}
	if params != nil {
		paramValues = params.data
	}
	body := map[string]any{
		"sql":    sql,
		"params": paramValues,
	}
	timeoutSec := 0
	if opts != nil {
		if opts.OffsetRows > 0 {
			body["offsetRows"] = opts.OffsetRows
		}
		if opts.LimitRows > 0 {
			body["limitRows"] = opts.LimitRows
		}
		timeoutSec = opts.TimeoutSec
	}

	resp, haveError := c.executor.Request(c.traceId, ctx, c.dbName, ep_QUERY, http.MethodPost, headers, body, timeoutSec, 2)
	if haveError != nil {
		return nil, haveError
	}

	return parseQueryResult(resp)
}

func (c *Client) Execute(ctx context.Context, sql string, params *Params) (*ExecuteResult, *HaveError) {

	headers := map[string]string{
		hEADER_DB_NAME: c.dbName,
	}
	paramValues := []paramValue{}
	if params != nil {
		paramValues = params.data
	}
	body := map[string]any{
		"sql":    sql,
		"params": paramValues,
	}

	resp, haveError := c.executor.Request(c.traceId, ctx, c.dbName, ep_EXECUTE, http.MethodPost, headers, body, 0, 2)
	if haveError != nil {
		return nil, haveError
	}

	var result ExecuteResult
	if err := json.NewDecoder(bytes.NewReader(resp.Body)).Decode(&result); err != nil {
		return nil, &HaveError{
			ErrCode: "ResponseBodyJSONDecodeError",
			Message: err.Error(),
		}
	}
	return &result, nil
}
