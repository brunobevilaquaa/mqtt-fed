package application

import (
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	paho "mqtt-fed/infra/queue"
	"os"
	"strconv"
	"time"
)

type FederatorContext struct {
	Id              int64
	CoreAnnInterval time.Duration
	BeaconInterval  time.Duration
	Redundancy      int
	CacheSize       int
	Neighbors       map[int64]*paho.Client
	HostClient      *paho.Client
}

type Federator struct {
	Ctx     *FederatorContext
	Workers map[string]*TopicWorkerHandle
}

func (f *Federator) Run() {
	topics := map[string]byte{
		TOPOLOGY_ANN_LEVEL: 2,
		CORE_ANNS:          2,
		MEMB_ANNS:          2,
		ROUTING_TOPICS:     2,
		FEDERATED_TOPICS:   2,
		BEACONS:            2,
	}

	var messageHandler mqtt.MessageHandler = func(client mqtt.Client, mqttMsg mqtt.Message) {
		msg, err := Deserialize(mqttMsg)

		federatedTopic := msg.Topic

		if err == nil {
			if msg.Type == "TopologyAnn" {
				if msg.TopologyAnn.Action == "NEW" {
					fmt.Println("Topology ann received, adding ", msg.TopologyAnn.Neighbor.Id, " to neighbors")
					mqttClient, err := paho.NewClient(msg.TopologyAnn.Neighbor.Ip, f.Ctx.HostClient.ClientID)

					if err == nil {
						f.Ctx.Neighbors[msg.TopologyAnn.Neighbor.Id] = mqttClient
					} else {
						fmt.Println(err)
					}

				} else if msg.TopologyAnn.Action == "REMOVE" {
					fmt.Println("Topology ann received, removing ", msg.TopologyAnn.Neighbor.Id, " from neighbors")
					delete(f.Ctx.Neighbors, msg.TopologyAnn.Neighbor.Id)
				}
			} else {
				if worker, ok := f.Workers[federatedTopic]; ok {
					worker.Dispatch(*msg)
				} else {
					worker := NewTopicWorkerHandle(federatedTopic, f.Ctx)
					worker.Dispatch(*msg)
					f.Workers[federatedTopic] = worker
				}
			}
		}
	}

	_, err := f.Ctx.HostClient.Consume(topics, messageHandler)

	if err != nil {
		panic(err)
	}
}

func Run(federatorConfig FederatorConfig) {
	clientId := "federator_" + strconv.FormatInt(federatorConfig.Id, 10)

	neighborsClients := createNeighborsClients(federatorConfig.Neighbors, clientId)
	hostClient := createHostClient(clientId)

	ctx := FederatorContext{
		Id:              federatorConfig.Id,
		CoreAnnInterval: federatorConfig.CoreAnnInterval,
		BeaconInterval:  federatorConfig.BeaconInterval,
		Redundancy:      federatorConfig.Redundancy,
		CacheSize:       1000,
		Neighbors:       neighborsClients,
		HostClient:      hostClient,
	}

	federator := Federator{
		Ctx:     &ctx,
		Workers: make(map[string]*TopicWorkerHandle),
	}

	federator.Run()
}

func createNeighborsClients(neighbors []NeighborConfig, clientId string) map[int64]*paho.Client {
	neighborsClients := make(map[int64]*paho.Client)

	for _, neighbor := range neighbors {
		mqttClient, err := paho.NewClient(neighbor.Ip, clientId)

		if err == nil {
			neighborsClients[neighbor.Id] = mqttClient
		} else {
			fmt.Println(err)
		}
	}

	return neighborsClients
}

func createHostClient(clientId string) *paho.Client {
	mosquittoPort := os.Getenv("MOSQUITTO_PORT")

	if mosquittoPort == "" {
		mosquittoPort = "1883"
	}

	mqttClient, err := paho.NewClient("tcp://localhost:"+mosquittoPort, clientId)

	if err != nil {
		panic(err)
	}

	return mqttClient
}
