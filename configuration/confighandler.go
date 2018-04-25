package configuration

import (
	"encoding/json"
	"fmt"
	"os"
)

var (
	DBConnectionDefault        = "127.0.0.1:27017"
	RedisCacheAddressDefault	= "127.0.0.1:6379"
	RedisCacheAddress2Default   = "127.0.0.1:6380"
)

type ServiceConfig struct {
	DBConnection        string         `json:"dbconnection"`
	RedisCacheAddress 	string			`json:"rediscacheaddress"`
	RedisCacheAddress2 string 			`json:"rediscacheaddress2"`
}

func ExtractConfiguration(filename string) (ServiceConfig, error) {
	conf := ServiceConfig{
		DBConnectionDefault,
		RedisCacheAddressDefault,
		RedisCacheAddress2Default,
	}

	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Configuration file not found. Continuing with default values.")
		// return conf, err
	}

	json.NewDecoder(file).Decode(&conf)

	if v := os.Getenv("MONGO_URL"); v != "" {
		conf.DBConnection = v
	}

	return conf, nil
}