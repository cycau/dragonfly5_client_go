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

	defaultDatabase = ""
	candidates := make([]nodeInfo, len(entries))

	var wg sync.WaitGroup
	for i, entry := range entries {
		n := &candidates[i]
		n.BaseURL = entry.BaseURL
		n.SecretKey = entry.SecretKey

		wg.Add(1)
		go func(i int, node *nodeInfo) {
			defer wg.Done()

			nodeInfo, err := fetchNodeInfo(node.BaseURL, node.SecretKey)
			if err != nil {
				log.Printf("### [Warning] Fetch node info %s: %v", node.BaseURL, err)
				return
			}
			node.NodeID = nodeInfo.NodeID
			node.Status = nodeInfo.Status
			node.MaxHttpQueue = nodeInfo.MaxHttpQueue
			node.Datasources = nodeInfo.Datasources
			node.CheckTime = time.Now()

			if defaultDatabase == "" {
				defaultDatabase = n.Datasources[0].DatabaseName
			}
		}(i, n)
	}
	wg.Wait()

	s.candidates = candidates

	s.maxConcurrency = semaphore.NewWeighted(int64(maxConcurrency * 8 / 10))
	s.tarConcurrency = semaphore.NewWeighted(int64(maxConcurrency * 2 / 10))

	return defaultDatabase, nil
}

// RequestTimeout は 0 のときクライアント共通タイムアウトのみ。>0 のときこのリクエスト専用のタイムアウトを指定（先に切れた方が有効）。
func (s *switcher) Request(ctx context.Context, dbName string, endpoint endpointType, method string, headers map[string]string, body map[string]any, timoutSec int, retryCount int) (*smartResponse, error) {
	if err := s.maxConcurrency.Acquire(ctx, 1); err != nil {
		return nil, err
	}

	nodeIdx, dsIdx, err := s.selectNode(dbName, endpoint)
	if err != nil {
		return nil, err
	}
	node := &s.candidates[nodeIdx]

	resp, err := s.requestHttp(ctx, nodeIdx, node.BaseURL, node.SecretKey, endpoint, method, headers, body, timoutSec, 2)
	// OK response
	if resp != nil && resp.StatusCode == http.StatusOK {
		httpRelease := resp.Release
		resp.Release = func() {
			httpRelease()
			s.maxConcurrency.Release(1)
		}
		return resp, nil
	}
	s.maxConcurrency.Release(1)

	// 他のノードを探すために、ノードの状態を更新
	var netErr net.Error
	if errors.As(err, &netErr) {
		node.Mu.Lock()
		node.Status = "DRAINING"
		node.CheckTime = time.Now()
		node.Mu.Unlock()
	} else if resp != nil && resp.StatusCode == http.StatusServiceUnavailable {
		node.Mu.Lock()
		node.Status = "DRAINING"
		node.CheckTime = time.Now()
		node.Mu.Unlock()
	} else if resp != nil && resp.StatusCode == http.StatusRequestTimeout {
		node.Mu.Lock()
		node.Datasources[dsIdx].Active = false
		node.CheckTime = time.Now()
		node.Mu.Unlock()
	} else {
		return nil, err
	}

	retryCount--
	if retryCount < 0 {
		return nil, fmt.Errorf("Failed to connect to service. Retry count exceeded. Last response: %+v, error: %v", resp, err)
	}
	time.Sleep(300 * time.Millisecond)

	// retry
	log.Printf("### Retry request to node %d, endpoint: %d, retryCount: %d, response: %+v, error: %v", nodeIdx, endpoint, retryCount, resp, err)
	return s.Request(ctx, dbName, endpoint, method, headers, body, timoutSec, retryCount)
}
func (s *switcher) RequestTargetNode(ctx context.Context, nodeIdx int, endpoint endpointType, method string, headers map[string]string, body map[string]any, timoutSec int, retryCount int) (*smartResponse, error) {

	node := &s.candidates[nodeIdx]
	resp, err := s.requestHttp(ctx, nodeIdx, node.BaseURL, node.SecretKey, endpoint, method, headers, body, timoutSec, 0)
	// OK response
	if resp != nil && resp.StatusCode == http.StatusOK {
		return resp, nil
	}

	// retry only when network error
	var netErr net.Error
	if !errors.As(err, &netErr) {
		return nil, err
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
		return nil, fmt.Errorf("Failed to connect to service. Retry count exceeded. Last response: %+v, error: %v", resp, err)
	}

	// retry
	log.Printf("### Retry request to node %d, endpoint: %d, retryCount: %d, response: %+v, error: %v", nodeIdx, endpoint, retryCount, resp, err)
	return s.RequestTargetNode(ctx, nodeIdx, endpoint, method, headers, body, timoutSec, retryCount)

}

type smartResponse struct {
	http.Response
	NodeIdx int
	Release func()
}

func (s *switcher) requestHttp(ctx context.Context, nodeIdx int, baseURL string, secretKey string, endpoint endpointType, method string, headers map[string]string, body any, timoutSec int, redirectCount int) (*smartResponse, error) {
	req, err := http.NewRequestWithContext(ctx, method, baseURL+"/rdb"+getEndpointPath(endpoint), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	traceId, ok := ctx.Value(ctx_TRACE_ID).(string)
	if ok {
		req.Header.Set("X-Request-Id", traceId)
	}
	req.Header.Set(hEADER_SECRET_KEY, secretKey)
	req.Header.Set(hEADER_REDIRECT_COUNT, strconv.Itoa(redirectCount))
	req.Header.Set(hEADER_TIMEOUT_SEC, strconv.Itoa(timoutSec))
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	if body != nil {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(&buf)
		req.ContentLength = int64(buf.Len())
	}
	resp, err := httpClient.Do(req)
	if resp == nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		sResponse := &smartResponse{
			Response: *resp,
			NodeIdx:  nodeIdx,
			Release: func() {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			},
		}
		return sResponse, nil
	}

	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	sResponse := &smartResponse{
		Response: *resp,
		NodeIdx:  nodeIdx,
	}
	if resp.StatusCode != http.StatusTemporaryRedirect {
		return sResponse, err
	}

	redirectCount--
	switch redirectCount {
	case 1:
		time.Sleep(300 * time.Millisecond)
	case 0:
		time.Sleep(900 * time.Millisecond)
	case -1:
		return nil, fmt.Errorf("Redirect count exceeded.")
	}

	redirectNodeId := resp.Header.Get("Location")
	if redirectNodeId == "" {
		return nil, fmt.Errorf("Redirect location is empty.")
	}

	for i := range s.candidates {
		redirectNode := &s.candidates[i]
		if redirectNode.NodeID == redirectNodeId {
			log.Printf("### Redirect to node %d from %d, RedirectCount: %d", i, nodeIdx, redirectCount)
			return s.requestHttp(ctx, i, redirectNode.BaseURL, redirectNode.SecretKey, endpoint, method, headers, body, timoutSec, redirectCount)
		}
	}

	return nil, fmt.Errorf("Redirect node id not found. Node[%s]", redirectNodeId)
}

const pROBLEMATIC_NODE_CHECK_INTERVAL = 15 * time.Second

func (s *switcher) selectNode(dbName string, endpoint endpointType) (nodeIdx int, dsIdx int, err error) {
	nodeIdx, dsIdx, problematicNodes, err := s.selectRandomNode(dbName, endpoint)

	if err != nil {
		// full scan and try again
		wg := sync.WaitGroup{}
		for _, i := range problematicNodes {
			n := &s.candidates[i]
			n.Mu.Lock()
			if time.Since(n.CheckTime) < pROBLEMATIC_NODE_CHECK_INTERVAL {
				n.Mu.Unlock()
				continue
			}

			wg.Add(1)
			go func(tarNode *nodeInfo) {
				defer tarNode.Mu.Unlock()
				defer wg.Done()

				nodeInfo, err := fetchNodeInfo(tarNode.BaseURL, tarNode.SecretKey)
				tarNode.CheckTime = time.Now()
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
		wg.Wait()

		nodeIdx, dsIdx, _, err = s.selectRandomNode(dbName, endpoint)
		return nodeIdx, dsIdx, err
	}

	// recover problematic nodes for next request
	for _, i := range problematicNodes {
		n := &s.candidates[i]
		n.Mu.Lock()
		if time.Since(n.CheckTime) < pROBLEMATIC_NODE_CHECK_INTERVAL {
			n.Mu.Unlock()
			continue
		}

		go func(tarNode *nodeInfo) {
			defer tarNode.Mu.Unlock()

			nodeInfo, err := fetchNodeInfo(tarNode.BaseURL, tarNode.SecretKey)
			tarNode.CheckTime = time.Now()
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

	return nodeIdx, dsIdx, err
}

type dsCandidate struct {
	nodeIdx int
	dsIdx   int
	weight  float64
}

func (s *switcher) selectRandomNode(dbName string, endpoint endpointType) (nodeIdx int, dsIdx int, problematicNodes []int, err error) {
	if len(s.candidates) == 0 {
		return -1, -1, nil, fmt.Errorf("Node Candidates are not initialized yet.")
	}

	var dsCandidates []dsCandidate
	var nodeIndexes []int

	for i := range s.candidates {
		n := &s.candidates[i]
		n.Mu.RLock()

		if n.Status != "SERVING" {
			if time.Since(n.CheckTime) > pROBLEMATIC_NODE_CHECK_INTERVAL {
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
			if ds.MaxWriteConns < 1 && (endpoint == ep_EXECUTE || endpoint == ep_TX_BEGIN) {
				continue
			}

			weight := 0.0
			switch endpoint {
			case ep_QUERY:
				weight = float64(ds.MaxConns - ds.MinWriteConns)
			case ep_EXECUTE:
				weight = float64(ds.MaxWriteConns)
			case ep_TX_BEGIN:
				weight = float64(ds.MaxWriteConns * 8 / 10)
			default:
				weight = float64(ds.MaxConns)
			}
			if weight <= 0 {
				continue
			}
			dsCandidates = append(dsCandidates, dsCandidate{nodeIdx: i, dsIdx: j, weight: weight})
		}
		n.Mu.RUnlock()

		if problematic && time.Since(n.CheckTime) > pROBLEMATIC_NODE_CHECK_INTERVAL {
			nodeIndexes = append(nodeIndexes, i)
		}
	}

	if len(dsCandidates) == 0 {
		return -1, -1, nodeIndexes, fmt.Errorf("No available datasource for database %s and endpoint type %d", dbName, endpoint)
	}
	total := 0.0
	for _, c := range dsCandidates {
		total += c.weight
	}
	r := rand.Float64() * total
	for _, cand := range dsCandidates {
		r -= cand.weight
		if r <= 0 {
			return cand.nodeIdx, cand.dsIdx, nodeIndexes, nil
		}
	}

	return -1, -1, nodeIndexes, fmt.Errorf("No available datasource for database %s and endpoint type %d.", dbName, endpoint)
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
