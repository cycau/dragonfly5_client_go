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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

/**************************************************
* TxClient
**************************************************/
type TxClient struct {
	dbName   string
	executor *switcher
	nodeIdx  int
	orgTxId  string
}

func NewTx(ctx context.Context, databaseName string, isolationLevel *IsolationLevel, maxTxTimeoutSec *int) (*TxClient, error) {
	if databaseName == "" {
		databaseName = dEFAULT_DATABASE
	}
	txId, nodeIdx, err := beginTx(ctx, databaseName, isolationLevel, maxTxTimeoutSec)
	if err != nil {
		return nil, err
	}

	return &TxClient{
		dbName:   databaseName,
		executor: executor,
		nodeIdx:  nodeIdx,
		orgTxId:  txId,
	}, nil
}

func RestoreTx(txId string) (*TxClient, error) {
	// Check size
	if len(txId) < 20 {
		return nil, fmt.Errorf("invalid txId size")
	}

	idx := strings.LastIndex(txId, ".")
	if idx == -1 {
		return nil, fmt.Errorf("invalid txId format")
	}
	nodeIdx, err := strconv.ParseInt(txId[idx+1:], 10, 8)
	if err != nil {
		return nil, err
	}

	return &TxClient{
		executor: executor,
		nodeIdx:  int(nodeIdx),
		orgTxId:  txId[:idx],
	}, nil
}

func (c *TxClient) GetTxId() string {
	return c.orgTxId + "." + strconv.Itoa(c.nodeIdx)
}

func beginTx(ctx context.Context, databaseName string, isolationLevel *IsolationLevel, maxTxTimeoutSec *int) (txId string, nodeIdx int, err error) {
	if err := executor.tarConcurrency.Acquire(ctx, 1); err != nil {
		return "", -1, err
	}
	defer executor.tarConcurrency.Release(1)

	headers := map[string]string{
		hEADER_DB_NAME: databaseName,
	}
	body := map[string]any{}
	if isolationLevel != nil {
		body["isolationLevel"] = *isolationLevel
	}
	if maxTxTimeoutSec != nil {
		body["maxTxTimeoutSec"] = *maxTxTimeoutSec
	}
	resp, err := executor.Request(ctx, databaseName, ep_TX_BEGIN, http.MethodPost, headers, body, 0, 2)
	if err != nil {
		return "", -1, err
	}
	defer resp.Release()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", -1, fmt.Errorf("begin tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	var result TxInfo
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", -1, err
	}

	return result.TxId, resp.NodeIdx, nil
}

func (c *TxClient) Query(ctx context.Context, sql string, params *Params, opts *QueryOptions) (*Records, error) {
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
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
		if opts.LimitRows > 0 {
			body["limitRows"] = opts.LimitRows
		}
		timeoutSec = opts.TimeoutSec
	}
	resp, err := c.executor.RequestTargetNode(ctx, c.nodeIdx, ep_TX_QUERY, http.MethodPost, headers, body, timeoutSec, 2)
	if err != nil {
		return nil, err
	}
	defer resp.Release()

	var result queryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return convertResult(result), nil
}

func (c *TxClient) Execute(ctx context.Context, sql string, params *Params) (*ExecuteResult, error) {
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	paramValues := []paramValue{}
	if params != nil {
		paramValues = params.data
	}
	body := map[string]any{
		"sql":    sql,
		"params": paramValues,
	}

	resp, err := c.executor.RequestTargetNode(ctx, c.nodeIdx, ep_TX_EXECUTE, http.MethodPost, headers, body, 0, 2)
	if err != nil {
		return nil, err
	}
	defer resp.Release()

	var result ExecuteResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *TxClient) Commit(ctx context.Context) error {
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	resp, err := c.executor.RequestTargetNode(ctx, c.nodeIdx, ep_TX_COMMIT, http.MethodPut, headers, nil, 0, 2)
	if err != nil {
		return err
	}
	defer resp.Release()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("commit tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (c *TxClient) Rollback(ctx context.Context) error {
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	resp, err := c.executor.RequestTargetNode(ctx, c.nodeIdx, ep_TX_ROLLBACK, http.MethodPut, headers, nil, 0, 2)
	if err != nil {
		return err
	}
	defer resp.Release()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("rollback tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (c *TxClient) Close(ctx context.Context) error {
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	resp, err := c.executor.RequestTargetNode(ctx, c.nodeIdx, ep_TX_CLOSE, http.MethodPut, headers, nil, 0, 3)
	if err != nil {
		return err
	}
	defer resp.Release()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("close tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}
