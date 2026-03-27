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
	"strconv"
	"strings"

	gonanoid "github.com/matoous/go-nanoid"
)

/**************************************************
* TxClient
**************************************************/
type TxClient struct {
	dbName   string
	executor *switcher
	nodeIdx  int
	orgTxId  string
	traceId  string
	ctx      context.Context
	isClosed bool
}

func NewTxDefault() (*TxClient, *HaveError) {
	return NewTx(context.Background(), "", "", 0, "")
}
func NewTx(ctx context.Context, databaseName string, isolationLevel IsolationLevel, maxTxTimeoutSec int, traceId string) (*TxClient, *HaveError) {
	if databaseName == "" {
		databaseName = dEFAULT_DATABASE
	}
	if traceId == "" {
		traceId, _ = gonanoid.Generate(ALPHA_NUM, 10)
		traceId = "d5" + traceId
	}

	txId, nodeIdx, haveError := beginTx(traceId, ctx, databaseName, isolationLevel, maxTxTimeoutSec)
	if haveError != nil {
		return nil, haveError
	}

	return &TxClient{
		dbName:   databaseName,
		executor: executor,
		nodeIdx:  nodeIdx,
		orgTxId:  txId,
		traceId:  traceId,
		ctx:      ctx,
		isClosed: false,
	}, nil
}
func RestoreTx(txId string, ctx context.Context) (*TxClient, error) {
	if txId == "" {
		return nil, fmt.Errorf("Transaction is already closed")
	}
	parts := strings.Split(txId, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid txId format")
	}
	nodeIdx, err := strconv.ParseInt(parts[1], 10, 8)
	if err != nil {
		return nil, err
	}
	if nodeIdx < 0 || nodeIdx >= int64(len(executor.candidates)) {
		return nil, fmt.Errorf("invalid node index in txId")
	}

	return &TxClient{
		executor: executor,
		nodeIdx:  int(nodeIdx),
		orgTxId:  parts[0],
		traceId:  parts[2],
		ctx:      ctx,
		isClosed: false,
	}, nil
}

func (c *TxClient) GetTxId() string {
	if c.isClosed {
		return ""
	}
	return c.orgTxId + "." + strconv.Itoa(c.nodeIdx) + "." + c.traceId
}

func beginTx(traceId string, ctx context.Context, databaseName string, isolationLevel IsolationLevel, maxTxTimeoutSec int) (txId string, nodeIdx int, haveError *HaveError) {
	if err := executor.tarConcurrency.Acquire(ctx, 1); err != nil {
		return "", -1, &HaveError{
			ErrCode: "TxConcurrencyAcquireError",
			Message: err.Error(),
		}
	}
	defer executor.tarConcurrency.Release(1)

	headers := map[string]string{
		hEADER_DB_NAME: databaseName,
	}
	body := map[string]any{}
	if isolationLevel != "" {
		body["isolationLevel"] = isolationLevel
	}
	if maxTxTimeoutSec > 0 {
		body["maxTxTimeoutSec"] = maxTxTimeoutSec
	}
	resp, haveError := executor.Request(traceId, ctx, databaseName, ep_TX_BEGIN, http.MethodPost, headers, body, 0, 2)
	if haveError != nil {
		return "", -1, haveError
	}

	var result TxInfo
	if err := json.NewDecoder(bytes.NewReader(resp.Body)).Decode(&result); err != nil {
		return "", -1, &HaveError{
			ErrCode: "ResponseBodyJSONDecodeError",
			Message: err.Error(),
		}
	}

	return result.TxId, resp.NodeIdx, nil
}

func (c *TxClient) Query(sql string, params *Params, opts *QueryOptions) (*Records, *HaveError) {
	if c.isClosed {
		return nil, &HaveError{
			ErrCode: "TxClosedError",
			Message: "Transaction is already closed",
		}
	}
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
		if opts.OffsetRows > 0 {
			body["offsetRows"] = opts.OffsetRows
		}
		if opts.LimitRows > 0 {
			body["limitRows"] = opts.LimitRows
		}
		timeoutSec = opts.TimeoutSec
	}
	resp, haveError := c.executor.RequestTargetNode(c.traceId, c.ctx, c.nodeIdx, ep_TX_QUERY, http.MethodPost, headers, body, timeoutSec, 2)
	if haveError != nil {
		return nil, haveError
	}

	return parseQueryResult(resp)
}

func (c *TxClient) Execute(sql string, params *Params) (*ExecuteResult, *HaveError) {
	if c.isClosed {
		return nil, &HaveError{
			ErrCode: "TxClosedError",
			Message: "Transaction is already closed",
		}
	}
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

	resp, haveError := c.executor.RequestTargetNode(c.traceId, c.ctx, c.nodeIdx, ep_TX_EXECUTE, http.MethodPost, headers, body, 0, 2)
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

func (c *TxClient) Commit() *HaveError {
	if c.isClosed {
		return &HaveError{
			ErrCode: "TxClosedError",
			Message: "Transaction is already closed",
		}
	}
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	_, haveError := c.executor.RequestTargetNode(c.traceId, c.ctx, c.nodeIdx, ep_TX_COMMIT, http.MethodPut, headers, nil, 0, 2)
	if haveError == nil {
		c.isClosed = true
	}
	return haveError
}

func (c *TxClient) Rollback() *HaveError {
	if c.isClosed {
		return &HaveError{
			ErrCode: "TxClosedError",
			Message: "Transaction is already closed",
		}
	}
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	_, haveError := c.executor.RequestTargetNode(c.traceId, c.ctx, c.nodeIdx, ep_TX_ROLLBACK, http.MethodPut, headers, nil, 0, 2)
	if haveError == nil {
		c.isClosed = true
	}
	return haveError
}

const max_PARAMS_PER_CHUNK = 20_000

func (c *TxClient) BulkInsert(tableName string, columnNames []string, records []*Params) (*ExecuteResult, *HaveError) {
	if c.isClosed {
		return nil, &HaveError{
			ErrCode: "TxClosedError",
			Message: "Transaction is already closed",
		}
	}
	if len(columnNames) == 0 {
		return nil, &HaveError{
			ErrCode: "InvalidArgument",
			Message: "columnNames must not be empty",
		}
	}
	if len(records) == 0 {
		return nil, &HaveError{
			ErrCode: "InvalidArgument",
			Message: "records must not be empty",
		}
	}

	colCount := len(columnNames)

	// Validate all records have correct param count
	for i, rec := range records {
		if len(rec.data) != colCount {
			return nil, &HaveError{
				ErrCode: "InvalidArgument",
				Message: fmt.Sprintf("record[%d] has %d params, expected %d", i, len(rec.data), colCount),
			}
		}
	}

	// Build column list and row placeholder once
	var colList strings.Builder
	colList.WriteString("INSERT INTO ")
	colList.WriteString(tableName)
	colList.WriteString(" (")
	for i, col := range columnNames {
		if i > 0 {
			colList.WriteString(", ")
		}
		colList.WriteString(col)
	}
	colList.WriteString(") VALUES ")
	sqlPrefix := colList.String()

	var rowPH strings.Builder
	rowPH.WriteString("(")
	for i := 0; i < colCount; i++ {
		if i > 0 {
			rowPH.WriteString(", ")
		}
		rowPH.WriteString("?")
	}
	rowPH.WriteString(")")
	rowPlaceholder := rowPH.String()

	// Chunk size: max rows per chunk based on maxParamsPerChunk
	rowsPerChunk := max_PARAMS_PER_CHUNK / colCount
	if rowsPerChunk < 1 {
		rowsPerChunk = 1
	}

	var totalEffected int64
	var totalElapsed int64

	totalRecords := len(records)
	log.Printf("### %s BulkInsert: totalRecords=%d", c.traceId, totalRecords)
	for offset := 0; offset < totalRecords; offset += rowsPerChunk {
		end := offset + rowsPerChunk
		if end > totalRecords {
			end = totalRecords
		}
		chunk := records[offset:end]

		// Build SQL
		var sb strings.Builder
		sb.WriteString(sqlPrefix)
		allParams := make([]paramValue, 0, colCount*len(chunk))
		for i, rec := range chunk {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(rowPlaceholder)
			allParams = append(allParams, rec.data...)
		}

		result, haveError := c.executeChunk(sb.String(), allParams)
		if haveError != nil {
			return nil, haveError
		}
		totalEffected += result.EffectedRows
		totalElapsed += result.ElapsedTimeUs

		log.Printf("### %s BulkInsert in progress: effectedRows=%d, elapsedTimeMs=%d", c.traceId, totalEffected, totalElapsed/1000)
	}

	return &ExecuteResult{
		EffectedRows:  totalEffected,
		ElapsedTimeUs: totalElapsed,
	}, nil
}

func (c *TxClient) executeChunk(sql string, params []paramValue) (*ExecuteResult, *HaveError) {
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	body := map[string]any{
		"sql":    sql,
		"params": params,
	}

	resp, haveError := c.executor.RequestTargetNode(c.traceId, c.ctx, c.nodeIdx, ep_TX_EXECUTE, http.MethodPost, headers, body, 0, 2)
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

func (c *TxClient) Close() *HaveError {
	if c.isClosed {
		return nil
	}
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	_, haveError := c.executor.RequestTargetNode(c.traceId, c.ctx, c.nodeIdx, ep_TX_ROLLBACK, http.MethodPut, headers, nil, 0, 3)
	if haveError == nil {
		c.isClosed = true
	} else {
		log.Printf("### TxClient Close Error: traceId=%s, txId=%s, nodeIdx=%d, error=%v", c.traceId, c.orgTxId, c.nodeIdx, haveError)
	}
	return haveError
}
