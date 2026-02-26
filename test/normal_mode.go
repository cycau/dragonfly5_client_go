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
	dbClient.Execute(
		context.Background(),
		"TRUNCATE TABLE users CASCADE",
		nil,
	)
	sql := "INSERT INTO users (id, name, email, password, icon, active, anonymous, email_verified, created_at, updated_at, last_logged_in_at)VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
	for i := range count {
		id := "0000000" + strconv.Itoa(i)
		id = id[len(id)-7:]
		params := rdbclient.NewParams().
			Add("d5794b1b-5f92-4dc6-aa48-085d_"+id, rdbclient.ValueType_STRING).
			Add("John Doe_"+id, rdbclient.ValueType_STRING).
			Add("john@example.com_"+id, rdbclient.ValueType_STRING).
			Add("password_"+id, rdbclient.ValueType_STRING).
			Add("https://example.com/static/icon_"+id, rdbclient.ValueType_STRING).
			Add(true, rdbclient.ValueType_BOOL).
			Add(true, rdbclient.ValueType_BOOL).
			Add(true, rdbclient.ValueType_BOOL).
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
		"SELECT * FROM users where id = $1",
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
		"SELECT * FROM users where id = $1",
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
		"UPDATE users SET email = $2 WHERE id = $1",
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
	defer txClient.Close()

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
		"UPDATE users SET email = $2 WHERE id = $1",
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
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Test error: %w", err)
	}

	if records.Get(0).Get("email") != email {
		return nil, fmt.Errorf("email mismatch: %w", err)
	}
	err = txClient.Commit()
	if err != nil {
		return nil, fmt.Errorf("CommitTx error: %w", err)
	}
	return records, nil
}
