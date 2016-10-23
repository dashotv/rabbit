package rabbit

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
)

type Subscriber struct {
	client *Client
	queue  amqp.Queue

	subscriptions map[string]sList
}

type sFunction func(string, map[string]string)

type sList []sFunction

func NewSubscriber(url string, exchangeName string, exchangeType string, queueName string) (*Subscriber, error) {
	var err error
	s := &Subscriber{}

	if s.client, err = NewClient(url); err != nil {
		return nil, err
	}

	if err = s.client.exchange(exchangeName, exchangeType); err != nil {
		return nil, err
	}

	if s.queue, err = s.client.queue(exchangeName, exchangeType, queueName); err != nil {
		return nil, err
	}

	s.subscriptions = make(map[string]sList)

	return s, nil
}

func (s *Subscriber) Add(name string, f sFunction) {
	s.subscriptions[name] = append(s.subscriptions[name], f)
}

func (s *Subscriber) Listen() {
	var err error

	go func() {
		var deliveries <-chan amqp.Delivery
		if deliveries, err = s.client.consume(s.queue); err != nil {
			fmt.Println("error consuming: ", err)
			return
		}

		//fmt.Println("ready for deliveries")
		for raw := range deliveries {
			// unmarshal and check subscriptions for name of message
			//fmt.Println("received: ", string(raw.Body))
			msg := &Message{}

			if err = json.Unmarshal(raw.Body, msg); err != nil {
				fmt.Println("error unmarshalling: ", err)
			}

			if _, ok := s.subscriptions[msg.Name]; ok {
				for _, f := range s.subscriptions[msg.Name] {
					go f(msg.Name, msg.Data)
				}
			}
		}
	}()
}
