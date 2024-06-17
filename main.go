package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v2"
	"mqtt-fed/application"
	"net/http"
	"os"
)

func main() {
	federatorConfig := getConfig()

	fmt.Println("Federator", federatorConfig.Id, "started!")

	application.Run(federatorConfig)

	select {}
}

func getConfig() application.FederatorConfig {
	var federatorConfig application.FederatorConfig

	if os.Getenv("TOPOLOGY_MANAGER_URL") != "" {
		body, _ := json.Marshal(&application.JoinRequest{
			Ip: os.Getenv("ADVERTISED_LISTENER"),
		})

		payload := bytes.NewBuffer(body)

		resp, err := http.Post(os.Getenv("TOPOLOGY_MANAGER_URL")+"/api/v1/join", "application/json", payload)

		if err != nil {
			panic(err)
		}

		var response application.HTTPResponse

		err = json.NewDecoder(resp.Body).Decode(&response)

		if err != nil {
			panic(err)
		}

		if response.Code != 200 {
			panic(response.Description)
		}

		federatorConfig, _ = response.Data.(application.FederatorConfig)
	} else if os.Getenv("CONFIG_FILE") != "" {
		data, err := os.ReadFile(os.Getenv("CONFIG_FILE"))
		if err != nil {
			panic(err)
		}

		err = yaml.Unmarshal(data, &federatorConfig)
		if err != nil {
			panic(err)
		}
	} else {
		panic("No configuration provided")
	}

	return federatorConfig
}
