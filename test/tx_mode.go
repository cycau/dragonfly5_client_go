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

func TestTxCase1() (*rdbclient.Records, error) {
	dbClient := rdbclient.Get("crm-system")

	txClient, err := rdbclient.NewTx(context.Background(), "crm-system", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("NewTx error: %w", err)
	}
	defer txClient.Close()

	idStr := "0000000" + strconv.Itoa(rand.Intn(1000000))
	userId := "d5794b1b-5f92-4dc6-aa48-085d_" + idStr[len(idStr)-7:]
	email := "john@example.com_alter_" + strconv.Itoa(rand.Intn(1000000))

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
		return nil, fmt.Errorf("Execute error: %w", err)
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
		return nil, fmt.Errorf("Query error: %w", err)
	}

	records2, err := dbClient.Query(
		context.Background(),
		"SELECT * FROM users where id = $1",
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

	err = txClient.Commit()
	if err != nil {
		return nil, fmt.Errorf("Commit error: %w", err)
	}

	records3, err := dbClient.Query(
		context.Background(),
		"SELECT * FROM users where id = $1",
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
	defer txClient.Close()

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
		"UPDATE users SET email = $2 WHERE id = $1",
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
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Query error: %w", err)
	}

	records2, err := dbClient.Query(
		context.Background(),
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Query error: %w", err)
	}

	if records.Get(0).Get("email") != email {
		return nil, fmt.Errorf("email mismatch: %w", err)
	}

	err = txClient.Rollback()
	if err != nil {
		return nil, fmt.Errorf("Rollback error: %w", err)
	}

	records3, err := dbClient.Query(
		context.Background(),
		"SELECT * FROM users where id = $1",
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
	//defer txClient.Close()

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
		"UPDATE users SET email = $2 WHERE id = $1",
		params,
	)

	time.Sleep(10 * time.Second)
	params = rdbclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", rdbclient.ValueType_STRING)
		Add(userId, rdbclient.ValueType_STRING)
	records, _ := txClient.Query(
		context.Background(),
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	time.Sleep(10 * time.Second)
	_, err = txClient.Query(
		context.Background(),
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	time.Sleep(10 * time.Second)
	_, err = txClient.Query(
		context.Background(),
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	time.Sleep(16 * time.Second)
	_, err = txClient.Query(
		context.Background(),
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("Query error: %w", err)
	}

	err = txClient.Rollback()

	return records, nil
}
