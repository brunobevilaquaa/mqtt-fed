package application

import (
	"encoding/json"
	"errors"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"strings"
)

const TOPOLOGY_ANN_LEVEL = "federator/topology_ann"

const BEACONS = "federator/beacon/#"
const BEACON_TOPIC_LEVEL = "federator/beacon/"

const CORE_ANNS = "federator/core_ann/#"
const CORE_ANN_TOPIC_LEVEL = "federator/core_ann/"

const MEMB_ANNS = "federator/memb_ann/#"
const MEMB_ANN_TOPIC_LEVEL = "federator/memb_ann/"

const FEDERATED_TOPICS = "federated/#"
const FEDERATED_TOPICS_LEVEL = "federated/"

const ROUTING_TOPICS = "federator/routing/#"
const ROUTING_TOPICS_LEVEL = "federator/routing/"

type Message struct {
	Topic string
	Type  string
	TopologyAnn
	FederatedPub
	RoutedPub
	CoreAnn
	MeshMembAnn
	Beacon
}

type TopologyAnn struct {
	Neighbor NeighborConfig `json:"neighbor"`
	Action   string         `json:"action"`
}

type RoutedPub struct {
	PubId    PubId
	SenderId int64
	Payload  []byte
}

type FederatedPub struct {
	Payload []byte
}

type CoreAnn struct {
	CoreId   int64
	SenderId int64
	Seqn     int
	Dist     int
}

type MeshMembAnn struct {
	CoreId   int64
	SenderId int64
	Seqn     int
}

type PubId struct {
	OriginId int64
	Seqn     int
}

type Beacon struct {
	Payload []byte
}

func Deserialize(mqttMessage mqtt.Message) (*Message, error) {
	topic := mqttMessage.Topic()

	message := Message{}

	var err error

	if strings.HasPrefix(topic, TOPOLOGY_ANN_LEVEL) {
		message.Type = "TopologyAnn"
		err = json.Unmarshal(mqttMessage.Payload(), &message.TopologyAnn)
	} else if strings.HasPrefix(topic, ROUTING_TOPICS_LEVEL) {
		message.Type = "RoutedPub"
		message.Topic = strings.TrimPrefix(topic, ROUTING_TOPICS_LEVEL)
		err = json.Unmarshal(mqttMessage.Payload(), &message.RoutedPub)
	} else if strings.HasPrefix(topic, FEDERATED_TOPICS_LEVEL) {
		message.Type = "FederatedPub"
		message.Topic = strings.TrimPrefix(topic, FEDERATED_TOPICS_LEVEL)
		message.FederatedPub.Payload = mqttMessage.Payload()
	} else if strings.HasPrefix(topic, CORE_ANN_TOPIC_LEVEL) {
		message.Type = "CoreAnn"
		message.Topic = strings.TrimPrefix(topic, CORE_ANN_TOPIC_LEVEL)
		err = json.Unmarshal(mqttMessage.Payload(), &message.CoreAnn)
	} else if strings.HasPrefix(topic, MEMB_ANN_TOPIC_LEVEL) {
		message.Type = "MeshMembAnn"
		message.Topic = strings.TrimPrefix(topic, MEMB_ANN_TOPIC_LEVEL)
		err = json.Unmarshal(mqttMessage.Payload(), &message.MeshMembAnn)
	} else if strings.HasPrefix(topic, BEACON_TOPIC_LEVEL) {
		message.Type = "Beacon"
		message.Topic = strings.TrimPrefix(topic, BEACON_TOPIC_LEVEL)
		message.Beacon.Payload = mqttMessage.Payload()
	} else {
		err = errors.New("received a packet from a topic it was not supposed to be subscribed to")
	}

	if err != nil {
		return nil, err
	}

	return &message, nil
}

func (f *FederatedPub) Serialize(fedTopic string) (string, []byte) {
	topic := CORE_ANN_TOPIC_LEVEL + fedTopic
	payload, _ := json.Marshal(&f)

	return topic, payload
}

func (r *RoutedPub) Serialize(fedTopic string) (string, []byte) {
	topic := ROUTING_TOPICS_LEVEL + fedTopic
	payload, _ := json.Marshal(&r)

	return topic, payload
}

func (c *CoreAnn) Serialize(fedTopic string) (string, []byte) {
	topic := CORE_ANN_TOPIC_LEVEL + fedTopic
	payload, _ := json.Marshal(&c)

	return topic, payload
}

func (m *MeshMembAnn) Serialize(fedTopic string) (string, []byte) {
	topic := MEMB_ANN_TOPIC_LEVEL + fedTopic
	payload, _ := json.Marshal(&m)

	return topic, payload
}
