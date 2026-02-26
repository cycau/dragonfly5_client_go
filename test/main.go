package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	rdbclient "github.com/cycau/dragonfly5_client_go/rdb"

	"golang.org/x/sync/semaphore"
)

func main() {
	err := rdbclient.InitWithYamlFile("config.yaml")
	if err != nil {
		fmt.Println("Configure error:", err)
		return
	}

	// _, err = test.TestTimeout()
	// if true {
	// 	fmt.Println("TestTimeout error:", err)
	// 	return
	// }

	if len(os.Args) > 1 && os.Args[1] == "m" {
		count, _ := strconv.ParseInt(os.Args[2], 10, 64)
		MakeTestData(int(count))
		return
	}

	count := 1
	batchSize := 1000
	if len(os.Args) > 1 {
		num, _ := strconv.ParseInt(os.Args[1], 10, 64)
		count = int(num)
	}
	if len(os.Args) > 2 {
		num, _ := strconv.ParseInt(os.Args[2], 10, 64)
		batchSize = int(num)
	}
	log.Printf("*** Starting test %d times with Concurrency %d\n", count, batchSize)

	var countOK, countError atomic.Int64
	start := time.Now()
	concurrency := semaphore.NewWeighted(int64(batchSize))
	wg := sync.WaitGroup{}
	for i := 0; i < count; i++ {
		concurrency.Acquire(context.Background(), 1)
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			defer concurrency.Release(1)

			var err error
			var result *rdbclient.Records
			funcIdx := rand.Intn(4)
			switch funcIdx {
			case 0:
				result, err = TestTxCase1()
			case 1:
				result, err = TestTxCase2()
			case 2:
				result, err = TestRandomSelect()
			case 3:
				_, err = TestRandomUpdate()
			case 4:
				result, err = TestRandomTx()
			}
			if err != nil {
				log.Printf("Test error: %v", err)
				countError.Add(1)
			}
			countOK.Add(1)
			if j == 0 {
				log.Printf(">>> Test result: %+v", result)
				log.Printf(">>> Time: %dms, OK: %d, Error: %d\n", time.Since(start).Milliseconds(), countOK.Load(), countError.Load())
			}
		}(i % batchSize)
	}
	wg.Wait()
	fmt.Printf("*** Total Time: %dms, OK: %d, Error: %d\n", time.Since(start).Milliseconds(), countOK.Load(), countError.Load())
}
