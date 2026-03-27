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
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type switcher struct {
	candidates     []nodeInfo
	maxConcurrency *semaphore.Weighted
	tarConcurrency *semaphore.Weighted
}

var executor = &switcher{}
var httpClient *http.Client

func (s *switcher) Init(entries []NodeEntry, maxConcurrency int, hosts int) (defaultDatabase string, err error) {

	httpClient = &http.Client{
		Timeout: 999 * time.Second, // 実質Clientからはタイムアウトさせない
		Transport: &http.Transport{
			MaxIdleConns:        maxConcurrency,         // 最大アイドル接続数（全ホスト合計）
			MaxIdleConnsPerHost: maxConcurrency / hosts, // ホストあたりのアイドル接続数
			MaxConnsPerHost:     maxConcurrency / hosts, // ホストあたりの同時接続上限
			DisableKeepAlives:   false,                  // Keep-Aliveを有効化

			DialContext: (&net.Dialer{ // 接続を確立する際のタイムアウト
				Timeout:   3 * time.Second,
				KeepAlive: 5 * time.Second,
			}).DialContext,
			IdleConnTimeout: 60 * time.Second, // アイドル接続のタイムアウト
		},
	}

	candidates := make([]nodeInfo, len(entries))

	for i, entry := range entries {
		n := &candidates[i]
		n.BaseURL = entry.BaseURL
		n.SecretKey = entry.SecretKey
		n.NextCheckAt = time.Now().Add(-1 * time.Hour)
	}
	s.candidates = candidates

	s.maxConcurrency = semaphore.NewWeighted(int64(maxConcurrency * 8 / 10))
	s.tarConcurrency = semaphore.NewWeighted(int64(maxConcurrency * 2 / 10))

	defaultDatabase, _ = s.syncAllNodeInfo()
	return defaultDatabase, nil
}

// RequestTimeout は 0 のときクライアント共通タイムアウトのみ。>0 のときこのリクエスト専用のタイムアウトを指定（先に切れた方が有効）。
func (s *switcher) Request(traceId string, ctx context.Context, dbName string, endpoint endpointType, method string, headers map[string]string, body map[string]any, timoutSec int, retryCount int) (*responseResult, *HaveError) {
	if err := s.maxConcurrency.Acquire(ctx, 1); err != nil {
		return nil, &HaveError{
			ErrCode: "ConcurrencyAcquireError",
			Message: err.Error(),
		}
	}

	nodeIdx, err := s.selectNode(traceId, dbName, endpoint)
	if err != nil {
		s.maxConcurrency.Release(1)
		return nil, &HaveError{
			ErrCode: "NodeSelectionError",
			Message: err.Error(),
		}
	}
	//log.Printf("### %s selected node %d", traceId, nodeIdx)
	node := &s.candidates[nodeIdx]

	resp, haveError := s.requestHttp(traceId, ctx, nodeIdx, node.BaseURL, node.SecretKey, endpoint, method, headers, body, timoutSec, 2)
	s.maxConcurrency.Release(1)
	// OK response
	if resp.StatusCode == http.StatusOK {
		return resp, nil
	}

	node = &s.candidates[resp.NodeIdx] // may changed after redirect
	// 他のノードを探すために、ノードの状態を更新
	switch resp.StatusCode {
	case 999:
		node.Mu.Lock()
		node.Status = haveError.ErrCode
		node.NextCheckAt = time.Now().Add(15 * time.Second)
		node.Mu.Unlock()
	case http.StatusTooManyRequests:
		node.Mu.Lock()
		node.Status = haveError.ErrCode
		node.NextCheckAt = time.Now().Add(5 * time.Second)
		node.Mu.Unlock()
	case http.StatusServiceUnavailable:
		node.Mu.Lock()
		node.NextCheckAt = time.Now().Add(1 * time.Second)
		for i := range node.Datasources {
			if node.Datasources[i].DatabaseName == dbName {
				node.Datasources[i].Active = false
			}
		}
		node.Mu.Unlock()
	default:
		return nil, haveError
	}

	retryCount--
	if retryCount < 0 {
		log.Printf("### %s Retry node: %d, endpoint: %d, retry exceeded, previous error: %+v", traceId, nodeIdx, endpoint, haveError)
		return nil, haveError
	}
	// switch to other node, so no need to adjust waiting time
	time.Sleep(300 * time.Millisecond)

	// retry
	log.Printf("### %s Retry node: %d, endpoint: %d, retryCounter: %d, previous error: %+v", traceId, nodeIdx, endpoint, retryCount, haveError)
	return s.Request(traceId, ctx, dbName, endpoint, method, headers, body, timoutSec, retryCount)
}
func (s *switcher) RequestTargetNode(traceId string, ctx context.Context, nodeIdx int, endpoint endpointType, method string, headers map[string]string, body map[string]any, timoutSec int, retryCount int) (*responseResult, *HaveError) {

	node := &s.candidates[nodeIdx]
	resp, haveError := s.requestHttp(traceId, ctx, nodeIdx, node.BaseURL, node.SecretKey, endpoint, method, headers, body, timoutSec, 0)
	// OK response
	if resp.StatusCode == http.StatusOK {
		return resp, nil
	}

	// retry only when network error
	if resp.StatusCode != 999 {
		return nil, haveError
	}

	retryCount--
	switch retryCount {
	case 2:
		time.Sleep(500 * time.Millisecond)
	case 1:
		time.Sleep(500 * time.Millisecond)
	case 0:
		time.Sleep(2500 * time.Millisecond)
	case -1:
		return nil, haveError
	}

	// retry
	log.Printf("### %s Retry target node: %d, endpoint: %d, retryCounter: %d, previous error: %+v", traceId, nodeIdx, endpoint, retryCount, haveError)
	return s.RequestTargetNode(traceId, ctx, nodeIdx, endpoint, method, headers, body, timoutSec, retryCount)

}

type responseResult struct {
	StatusCode  int
	NodeIdx     int
	ContentType string
	Body        []byte
}

type HaveError struct {
	ErrCode string `json:"errcode"`
	Message string `json:"message"`
}

func (s *switcher) requestHttp(traceId string, ctx context.Context, nodeIdx int, baseURL string, secretKey string, endpoint endpointType, method string, headers map[string]string, body any, timoutSec int, redirectCount int) (*responseResult, *HaveError) {
	req, err := http.NewRequestWithContext(ctx, method, baseURL+"/rdb"+getEndpointPath(endpoint), nil)
	if err != nil {
		return &responseResult{
				StatusCode: -1,
				NodeIdx:    nodeIdx,
			}, &HaveError{
				ErrCode: "CreateRequestWithContextError",
				Message: err.Error(),
			}
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-Trace-Id", traceId)
	req.Header.Set(hEADER_SECRET_KEY, secretKey)
	req.Header.Set(hEADER_REDIRECT_COUNT, strconv.Itoa(redirectCount))
	req.Header.Set(hEADER_TIMEOUT_SEC, strconv.Itoa(timoutSec))
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	if body != nil {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return &responseResult{
					StatusCode: -1,
					NodeIdx:    nodeIdx,
				}, &HaveError{
					ErrCode: "RequestBodyJSONEncodingError",
					Message: err.Error(),
				}
		}
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		req.ContentLength = int64(len(bodyBytes))
	}

	resp, err := httpClient.Do(req)
	var bodyBytes []byte
	var bodyErr error
	if resp != nil && resp.Body != nil {
		bodyBytes, bodyErr = io.ReadAll(resp.Body) // 読み切るのが大事
		resp.Body.Close()
	}

	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) {
			return &responseResult{
					StatusCode: 999, // ネットワークエラー
					NodeIdx:    nodeIdx,
				}, &HaveError{
					ErrCode: "NETWORK_ERROR",
					Message: err.Error(),
				}
		}

		return &responseResult{
				StatusCode: -1,
				NodeIdx:    nodeIdx,
			}, &HaveError{
				ErrCode: "HTTPRequestError",
				Message: err.Error(),
			}
	}
	if resp == nil {
		return &responseResult{
				StatusCode: -1,
				NodeIdx:    nodeIdx,
			}, &HaveError{
				ErrCode: "HTTPRequestError",
				Message: "Unexpected response(nil)",
			}
	}
	if resp.StatusCode == http.StatusOK {
		if bodyErr != nil {
			return &responseResult{
					StatusCode: -1,
					NodeIdx:    nodeIdx,
				}, &HaveError{
					ErrCode: "ResponseBodyReadError",
					Message: bodyErr.Error(),
				}
		}
		return &responseResult{
			StatusCode:  resp.StatusCode,
			NodeIdx:     nodeIdx,
			ContentType: resp.Header.Get("Content-Type"),
			Body:        bodyBytes,
		}, nil
	}

	if resp.StatusCode != http.StatusTemporaryRedirect {
		var result HaveError
		if err := json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(&result); err != nil {
			return &responseResult{
					StatusCode: -1,
					NodeIdx:    nodeIdx,
				}, &HaveError{
					ErrCode: "ResponseErrorJSONDecodingError",
					Message: fmt.Sprintf("Status code: %d. Failed to decode error response body: %v", resp.StatusCode, err),
				}
		}
		if result.ErrCode == "" {
			result.ErrCode = fmt.Sprintf("HTTP_%d", resp.StatusCode)
			result.Message = string(bodyBytes)
		}
		return &responseResult{
				StatusCode: resp.StatusCode,
				NodeIdx:    nodeIdx,
			}, &HaveError{
				ErrCode: result.ErrCode,
				Message: result.Message,
			}
	}

	redirectCount--
	switch redirectCount {
	case 1:
		time.Sleep(300 * time.Millisecond)
	case 0:
		time.Sleep(900 * time.Millisecond)
	case -1: // Not be happened because server side also checks redirect count, but just in case
		return &responseResult{
				StatusCode: -1,
				NodeIdx:    nodeIdx,
			}, &HaveError{
				ErrCode: "RedirectCountExceeded",
				Message: "Redirect count exceeded.",
			}
	}

	redirectNodeId := resp.Header.Get("Location")
	if redirectNodeId == "" {
		return &responseResult{
				StatusCode: -1,
				NodeIdx:    nodeIdx,
			}, &HaveError{
				ErrCode: "RedirectLocationEmpty",
				Message: "Redirect location is empty.",
			}
	}

	for i := range s.candidates {
		redirectNode := &s.candidates[i]
		redirectNode.Mu.RLock()
		if redirectNode.NodeID == redirectNodeId {
			redirectNode.Mu.RUnlock()
			log.Printf("### %s Redirect to node %d from %d, redirectCounter: %d", traceId, i, nodeIdx, redirectCount)
			return s.requestHttp(traceId, ctx, i, redirectNode.BaseURL, redirectNode.SecretKey, endpoint, method, headers, body, timoutSec, redirectCount)
		}
		redirectNode.Mu.RUnlock()
	}

	log.Printf("### %s redirect node not found, sync all nodes and try again", traceId)
	s.syncAllNodeInfo()

	for i := range s.candidates {
		redirectNode := &s.candidates[i]
		redirectNode.Mu.RLock()
		if redirectNode.NodeID == redirectNodeId {
			redirectNode.Mu.RUnlock()
			log.Printf("### %s Redirect to node %d from %d, redirectCounter: %d", traceId, i, nodeIdx, redirectCount)
			return s.requestHttp(traceId, ctx, i, redirectNode.BaseURL, redirectNode.SecretKey, endpoint, method, headers, body, timoutSec, redirectCount)
		}
		redirectNode.Mu.RUnlock()
	}

	return &responseResult{
			StatusCode: -1,
			NodeIdx:    nodeIdx,
		}, &HaveError{
			ErrCode: "RedirectNodeNotFound",
			Message: fmt.Sprintf("Redirect node id not found. Node[%s]", redirectNodeId),
		}
}

func (s *switcher) selectNode(traceId string, dbName string, endpoint endpointType) (nodeIdx int, err error) {
	nodeIdx, problematicNodes, err := s.selectRandomNode(traceId, dbName, endpoint)

	if err != nil {
		// full scan and try again
		_, syncCnt := s.syncAllNodeInfo()
		nodeIdx, _, err = s.selectRandomNode(traceId, dbName, endpoint)
		if syncCnt > 0 {
			log.Printf("[selectNode]: sync all nodes[%d] and try again result: nodeIdx: %d, err: %v", syncCnt, nodeIdx, err)
		}
		return nodeIdx, err
	}

	// recover problematic nodes for next request
	for _, i := range problematicNodes {
		n := &s.candidates[i]
		n.Mu.Lock()
		if n.NextCheckAt.After(time.Now()) {
			n.Mu.Unlock()
			continue
		}
		n.NextCheckAt = time.Now().Add(15 * time.Second)
		n.Mu.Unlock()

		go func(tarNode *nodeInfo) {
			nodeInfo, err := fetchNodeInfo(tarNode.BaseURL, tarNode.SecretKey)

			tarNode.Mu.Lock()
			defer tarNode.Mu.Unlock()
			if err != nil {
				log.Printf("[selectNode] Failed to fetch node info %s: %v", tarNode.BaseURL, err)
				tarNode.Status = "HEALZERR"
				return
			}
			tarNode.NodeID = nodeInfo.NodeID
			tarNode.Status = nodeInfo.Status
			tarNode.MaxHttpQueue = nodeInfo.MaxHttpQueue
			tarNode.Datasources = nodeInfo.Datasources
		}(n)
	}

	return nodeIdx, err
}

type dsCandidate struct {
	nodeIdx int
	dsIdx   int
	weight  float64
}

func (s *switcher) selectRandomNode(traceId string, dbName string, endpoint endpointType) (nodeIdx int, problematicNodes []int, err error) {
	if len(s.candidates) == 0 {
		return -1, nil, fmt.Errorf("Node Candidates are not initialized yet.")
	}

	var dsCandidates []dsCandidate
	var nodeIndexes []int

	for i := range s.candidates {
		n := &s.candidates[i]
		n.Mu.RLock()

		if n.Status != "SERVING" {
			if n.NextCheckAt.Before(time.Now()) {
				nodeIndexes = append(nodeIndexes, i)
			}
			n.Mu.RUnlock()
			continue
		}

		problematic := false
		for j := range n.Datasources {
			ds := &n.Datasources[j]
			if ds.DatabaseName != dbName {
				continue
			}
			if !ds.Active {
				problematic = true
				continue
			}
			if endpoint == ep_QUERY && (ds.MaxConns-ds.MinWriteConns < 1) {
				continue
			}
			if endpoint == ep_EXECUTE && ds.MaxWriteConns < 1 {
				continue
			}
			if endpoint == ep_TX_BEGIN && ds.MaxWriteConns < 1 {
				continue
			}

			weight := 0.0
			switch endpoint {
			case ep_QUERY:
				weight = float64(ds.MaxConns - ds.MinWriteConns)
			case ep_EXECUTE:
				weight = float64(ds.MaxWriteConns)
			case ep_TX_BEGIN:
				weight = float64(ds.MaxWriteConns * 7 / 10)
			default:
				weight = float64(ds.MaxConns)
			}
			if weight <= 0 {
				continue
			}
			dsCandidates = append(dsCandidates, dsCandidate{nodeIdx: i, dsIdx: j, weight: weight})
		}

		if problematic && n.NextCheckAt.Before(time.Now()) {
			nodeIndexes = append(nodeIndexes, i)
		}
		n.Mu.RUnlock()
	}

	if len(dsCandidates) == 0 {
		log.Printf("### %s [selectNode]: No available datasource for database[%s] and endpoint type[%d], candidates: %v", traceId, dbName, endpoint, s.candidates)
		return -1, nodeIndexes, fmt.Errorf("No available datasource for database[%s] and endpoint type[%d]", dbName, endpoint)
	}
	total := 0.0
	for _, c := range dsCandidates {
		total += c.weight
	}
	r := rand.Float64() * total
	for _, cand := range dsCandidates {
		r -= cand.weight
		if r <= 0 {
			return cand.nodeIdx, nodeIndexes, nil
		}
	}

	last := dsCandidates[len(dsCandidates)-1]
	return last.nodeIdx, nodeIndexes, nil
}

func (s *switcher) syncAllNodeInfo() (defaultDatabase string, syncCnt int) {

	cnt := 0
	wg := sync.WaitGroup{}
	for i := range s.candidates {
		n := &s.candidates[i]
		n.Mu.Lock()
		if n.NextCheckAt.After(time.Now()) {
			n.Mu.Unlock()
			continue
		}
		n.NextCheckAt = time.Now().Add(15 * time.Second) // prevent duplicate fetches
		n.Mu.Unlock()

		cnt++
		wg.Add(1)
		go func(tarNode *nodeInfo) {
			defer wg.Done()

			nodeInfo, err := fetchNodeInfo(tarNode.BaseURL, tarNode.SecretKey)

			tarNode.Mu.Lock()
			defer tarNode.Mu.Unlock()
			if err != nil {
				log.Printf("### [Warning] Failed to fetch node info %s: %v", tarNode.BaseURL, err)
				tarNode.Status = "HEALZERR"
				return
			}
			tarNode.NodeID = nodeInfo.NodeID
			tarNode.Status = nodeInfo.Status
			tarNode.MaxHttpQueue = nodeInfo.MaxHttpQueue
			tarNode.Datasources = nodeInfo.Datasources
		}(n)
	}
	wg.Wait()

	if len(s.candidates) < 1 || len(s.candidates[0].Datasources) < 1 {
		return "", cnt
	}
	return s.candidates[0].Datasources[0].DatabaseName, cnt
}

func fetchNodeInfo(baseURL string, secretKey string) (*nodeInfo, error) {

	req, err := http.NewRequest("GET", baseURL+"/healz", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(hEADER_SECRET_KEY, secretKey)
	resp, err := httpClient.Do(req)

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var nodeInfo nodeInfo
	err = json.Unmarshal(body, &nodeInfo)
	if err != nil {
		return nil, err
	}

	return &nodeInfo, nil
}
