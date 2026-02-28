package main

import (
	"log"
	"testing"

	rdbclient "github.com/cycau/dragonfly5_client_go/rdb"
)

func TestRush(t *testing.T) {
	err := rdbclient.InitWithYamlFile("config.yaml")
	if err != nil {
		log.Fatal("Configure error:", err)
	}

	MakeTestData(10000)
	runRush(10000, 1000)
}
