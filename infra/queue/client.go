package queue

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Client struct {
	ClientID string
	client   mqtt.Client
}

func NewClient(broker string, clientID string) (*Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(clientID)
	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return &Client{
		ClientID: clientID,
		client:   client,
	}, nil
}

func (c Client) Consume(topics map[string]byte, messageHandler mqtt.MessageHandler) (bool, error) {
	token := c.client.SubscribeMultiple(topics, messageHandler)
	token.Wait()

	if token.Error() != nil {
		return false, token.Error()
	}

	return true, nil
}

func (c Client) Publish(topic string, message string, qos byte, retained bool) (bool, error) {
	token := c.client.Publish(topic, qos, retained, message)
	token.Wait()

	if token.Error() != nil {
		return false, token.Error()
	}

	return true, nil
}

func (c Client) Disconnect() {
	c.client.Disconnect(10)
}
