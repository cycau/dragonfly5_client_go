package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	rdbclient "github.com/cycau/dragonfly5_client_go/rdb"
)

// サンプル用の一時 config を作成し、Init が config を読むことと
// Get が未初期化でエラーになることを確認する
func MakeTestData(count int) {
	log.Println("MakeTestData start")

	dbClient := rdbclient.Get("crm-system")
	_, err := dbClient.Execute(
		context.Background(),
		"TRUNCATE TABLE test_user",
		nil,
	)
	if err != nil {
		_, err = dbClient.Execute(
			context.Background(),
			`CREATE TABLE test_user (
				id VARCHAR(50) PRIMARY KEY, 
				nickname VARCHAR(50), 
				email VARCHAR(50), 
				email_verified BOOLEAN, 
				status VARCHAR(10),
				password VARCHAR(200), 
				icon VARCHAR(200), 
				created_at timestamp(3), 
				updated_at timestamp(3), 
				last_logged_in_at timestamp(3)
			)`,
			nil,
		)
		if err != nil {
			log.Printf("CreateTable error: %v\n", err)
			return
		}
	}

	sql := `
	INSERT INTO test_user 
			(id, nickname, email, email_verified, status, password, icon, created_at, updated_at, last_logged_in_at)
	VALUES	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	for i := range count {
		id := "0000000" + strconv.Itoa(i)
		id = id[len(id)-7:]
		params := rdbclient.NewParams().
			Add("d5794b1b-5f92-4dc6-aa48-085d_"+id, rdbclient.ValueType_STRING).
			Add("John Doe_"+id, rdbclient.ValueType_STRING).
			Add("john@example.com_"+id, rdbclient.ValueType_STRING).
			Add(true, rdbclient.ValueType_BOOL).
			Add("ACTIVE", rdbclient.ValueType_STRING).
			Add("password_d5794b1b-5f92-4dc6-aa48-085d_d5794b1b-5f92-4dc6-aa48-085d_"+id, rdbclient.ValueType_STRING).
			Add("https://example.com/static/icon_d5794b1b-5f92-4dc6-aa48-085d_"+id, rdbclient.ValueType_STRING).
			Add(time.Now(), rdbclient.ValueType_DATETIME).
			Add(time.Now(), rdbclient.ValueType_DATETIME).
			Add(time.Now(), rdbclient.ValueType_DATETIME)

		_, err := dbClient.Execute(
			context.Background(),
			sql,
			params,
		)
		if err != nil {
			log.Printf("MakeTestData error: %v\n", err)
			return
		}
		if i%10000 == 0 {
			log.Printf("MakeTestData progress: %d / %d", i, count)
		}
	}
}

func TestTimeout() (*rdbclient.Records, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	dbClient := rdbclient.Get("crm-system")
	id := rand.Intn(1000000)
	idStr := "0000000" + strconv.Itoa(id)
	idStr = idStr[len(idStr)-7:]
	params := rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add("d5794b1b-5f92-4dc6-aa48-085d_"+idStr, rdbclient.ValueType_STRING)

	records, err := dbClient.Query(
		ctx,
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("TestXXX error: %w", err)
	}
	log.Printf("TestXXX result: %s %s\n", idStr, records.Get(0).Get("id"))

	return records, nil
}
func TestRandomSelect() (*rdbclient.Records, error) {

	dbClient := rdbclient.Get("crm-system")
	id := rand.Intn(1000000)
	idStr := "0000000" + strconv.Itoa(id)
	idStr = idStr[len(idStr)-7:]
	params := rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add("d5794b1b-5f92-4dc6-aa48-085d_"+idStr, rdbclient.ValueType_STRING)

	records, err := dbClient.Query(
		context.Background(),
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Test error: %w", err)
	}
	//fmt.Printf("Test1 result: %s %s\n", idStr, records.Get(0).Get("id"))

	return records, nil
}
func TestRandomUpdate() (*rdbclient.ExecuteResult, error) {

	dbClient := rdbclient.Get("crm-system")
	id := rand.Intn(1000000)
	idStr := "0000000" + strconv.Itoa(id)
	idStr = idStr[len(idStr)-7:]
	params := rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add("d5794b1b-5f92-4dc6-aa48-085d_"+idStr, rdbclient.ValueType_STRING).
		Add("john@example.com_"+idStr, rdbclient.ValueType_STRING)

	result, err := dbClient.Execute(
		context.Background(),
		"UPDATE test_user SET email = $2 WHERE id = $1",
		params,
	)
	if err != nil {
		return nil, fmt.Errorf("TestUpdate error: %w", err)
	}
	return result, nil
}
func TestRandomTx() (*rdbclient.Records, error) {

	txClient, err := rdbclient.NewTx(context.Background(), "crm-system", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("NewTx error: %w", err)
	}
	defer txClient.Close(context.Background())

	id := rand.Intn(1000000)
	idStr := "0000000" + strconv.Itoa(id)
	idStr = idStr[len(idStr)-7:]
	userId := "d5794b1b-5f92-4dc6-aa48-085d_" + idStr
	email := "john@example.com_Tx_" + idStr

	params := rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add(userId, rdbclient.ValueType_STRING).
		Add(email, rdbclient.ValueType_STRING)

	_, err = txClient.Execute(
		context.Background(),
		"UPDATE test_user SET email = $2 WHERE id = $1",
		params,
	)
	if err != nil {
		return nil, fmt.Errorf("TestUpdate error: %w", err)
	}

	params = rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add(userId, rdbclient.ValueType_STRING)

	records, err := txClient.Query(
		context.Background(),
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Test error: %w", err)
	}

	if records.Get(0).Get("email") != email {
		return nil, fmt.Errorf("email mismatch: %w", err)
	}
	err = txClient.Commit(context.Background())
	if err != nil {
		return nil, fmt.Errorf("CommitTx error: %w", err)
	}
	return records, nil
}

func TestTxCase1() (*rdbclient.Records, error) {
	dbClient := rdbclient.Get("crm-system")

	ctx := rdbclient.SetTraceId(context.Background(), "TestTxCase1")
	txClient, err := rdbclient.NewTx(ctx, "crm-system", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("NewTx error: %w", err)
	}
	defer txClient.Close(ctx)

	idStr := "0000000" + strconv.Itoa(rand.Intn(1000000))
	userId := "d5794b1b-5f92-4dc6-aa48-085d_" + idStr[len(idStr)-7:]
	email := "john@example.com_alter_" + strconv.Itoa(rand.Intn(1000000))

	params := rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add(userId, rdbclient.ValueType_STRING).
		Add(email, rdbclient.ValueType_STRING)
	_, err = txClient.Execute(
		ctx,
		"UPDATE test_user SET email = $2 WHERE id = $1",
		params,
	)
	if err != nil {
		return nil, fmt.Errorf("Execute error: %w", err)
	}

	params = rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add(userId, rdbclient.ValueType_STRING)
	records, err := txClient.Query(
		ctx,
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Query error: %w", err)
	}

	records2, err := dbClient.Query(
		ctx,
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Query error: %w", err)
	}

	if records.Get(0).Get("email") != email {
		return nil, fmt.Errorf("email should BE the same: %s != %s", records.Get(0).Get("email"), email)
	}
	if records2.Get(0).Get("email") == email {
		return nil, fmt.Errorf("email should NOT BE the same: %s == %s", records2.Get(0).Get("email"), email)
	}

	err = txClient.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("Commit error: %w", err)
	}

	records3, err := dbClient.Query(
		ctx,
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)

	//log.Println(records.Get(0).Get("email"), records2.Get(0).Get("email"), records3.Get(0).Get("email"))

	return records3, nil
}
func TestTxCase2() (*rdbclient.Records, error) {
	maxTxTimeoutSec := 60
	dbClient := rdbclient.Get("crm-system")

	txClient, err := rdbclient.NewTx(context.Background(), "crm-system", nil, &maxTxTimeoutSec)
	if err != nil {
		return nil, fmt.Errorf("NewTx error: %w", err)
	}
	defer txClient.Close(context.Background())

	id := rand.Intn(1000000)
	idStr := "0000000" + strconv.Itoa(id)
	idStr = idStr[len(idStr)-7:]
	userId := "d5794b1b-5f92-4dc6-aa48-085d_" + idStr
	email := "john@example.com_alter_" + idStr

	params := rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add(userId, rdbclient.ValueType_STRING).
		Add(email, rdbclient.ValueType_STRING)
	_, err = txClient.Execute(
		context.Background(),
		"UPDATE test_user SET email = $2 WHERE id = $1",
		params,
	)
	if err != nil {
		return nil, fmt.Errorf("Execute error: %w", err)
	}

	params = rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add(userId, rdbclient.ValueType_STRING)
	records, err := txClient.Query(
		context.Background(),
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Query error: %w", err)
	}

	records2, err := dbClient.Query(
		context.Background(),
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Query error: %w", err)
	}

	if records.Get(0).Get("email") != email {
		return nil, fmt.Errorf("email mismatch: %w", err)
	}

	err = txClient.Rollback(context.Background())
	if err != nil {
		return nil, fmt.Errorf("Rollback error: %w", err)
	}

	records3, err := dbClient.Query(
		context.Background(),
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Query error: %w", err)
	}

	if id == 0 {
		log.Println(records.Get(0).Get("email"), records2.Get(0).Get("email"), records3.Get(0).Get("email"))
	}

	return records3, nil
}
func TestTxCase3() (*rdbclient.Records, error) {

	txClient, err := rdbclient.NewTx(context.Background(), "crm-system", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("NewTx error: %w", err)
	}
	defer txClient.Close(context.Background())

	id := rand.Intn(1000000)
	idStr := "0000000" + strconv.Itoa(id)
	idStr = idStr[len(idStr)-7:]
	userId := "d5794b1b-5f92-4dc6-aa48-085d_" + idStr
	email := "john@example.com_TxTimeout_" + idStr

	params := rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add(userId, rdbclient.ValueType_STRING).
		Add(email, rdbclient.ValueType_STRING)
	_, err = txClient.Execute(
		context.Background(),
		"UPDATE test_user SET email = $2 WHERE id = $1",
		params,
	)

	time.Sleep(10 * time.Second)
	params = rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add(userId, rdbclient.ValueType_STRING)
	records, _ := txClient.Query(
		context.Background(),
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)

	time.Sleep(10 * time.Second)
	_, err = txClient.Query(
		context.Background(),
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)

	time.Sleep(10 * time.Second)
	_, err = txClient.Query(
		context.Background(),
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)

	time.Sleep(16 * time.Second)
	_, err = txClient.Query(
		context.Background(),
		"SELECT * FROM test_user where id = $1",
		params,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("Query error: %w", err)
	}

	err = txClient.Rollback(context.Background())

	return records, nil
}
