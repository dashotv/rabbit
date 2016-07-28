package rabbit

import (
	"github.com/streadway/amqp"
	"fmt"
)

var ()

type Client struct {
	Url        string
	Connected  bool
	Connection *amqp.Connection
	Channel    *amqp.Channel
}

func NewClient(url string) (*Client, error) {
	var err error

	c := &Client{Url: url, Connected: false }

	if c.Connection, err = amqp.Dial(c.Url); err != nil {
		return nil, err
	}

	if c.Channel, err = c.Connection.Channel(); err != nil {
		return nil, err
	}

	c.Connected = true
	return c, nil
}

func (c *Client) Close() {
	c.Connection.Close()
}

//func (c *Client) Publisher(exchangeName, exchangeType string, msg []byte) {
//	// just publish one message?
//}

//func (c *Client) Subscriber(exchangeName, exchangeType, queueName string) (*Subscriber, error) {
//	var queue amqp.Queue
//	var err error
//
//	if err = c.exchange(exchangeName, exchangeType); err != nil {
//		return nil, err
//	}
//
//	if queue, err = c.queue(exchangeName, exchangeType, queueName); err != nil {
//		return nil, err
//	}
//
//	return NewSubscriber(c, queue), nil
//}

func (c *Client) Producer(exchangeName string, exchangeType string) (chan []byte, error) {
	publishing := make(chan []byte, 1)

	if err := c.exchange(exchangeName, exchangeType); err != nil {
		return nil, err
	}

	// add concurrency
	// loop to make more goroutines based on int arg
	go func() {
		for msg := range publishing {
			//var data []byte
			var err error

			if err = c.publish(exchangeName, msg); err != nil {
				fmt.Println("error publishing: ", err)
				return
			}
		}
	}()

	return publishing, nil
}

func (c *Client) Consumer(exchangeName string, exchangeType string, queueName string) (chan []byte, error) {
	consuming := make(chan []byte, 1)
	var queue amqp.Queue
	var err error

	if err = c.exchange(exchangeName, exchangeType); err != nil {
		return nil, err
	}

	if queue, err = c.queue(exchangeName, exchangeType, queueName); err != nil {
		return nil, err
	}

	go func() {
		var deliveries <-chan amqp.Delivery
		if deliveries, err = c.consume(queue); err != nil {
			fmt.Println("error consuming: ", err)
			return
		}

		for raw := range deliveries {
			//fmt.Println("consume raw: ", string(raw.Body))
			consuming <- raw.Body
		}
	}()

	return consuming, nil
}

func (c *Client) exchange(exchangeName, exchangeType string) error {
	return c.Channel.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type
		true, // durable
		false, // auto-deleted
		false, // internal
		false, // noWait
		nil, // arguments
	)
}

func (c *Client) publish(exchangeName string, data []byte) error {
	return c.Channel.Publish(
		exchangeName, // publish to an exchange
		"", // routing to 0 or more queues
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			//Headers:         amqp.Table{},
			//ContentType:     "text/plain",
			//ContentEncoding: "",
			Body:            data,
			//DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			//Priority:        0, // 0-9
			// a bunch of application/implementation-specific fields
		},
	)
}

func (c *Client) consume(queue amqp.Queue) (<-chan amqp.Delivery, error) {
	return c.Channel.Consume(
		queue.Name, // name
		"",      // consumerTag,
		true,      // AutoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
}

func (c *Client) queue(exchangeName, exchangeType, queueName string) (queue amqp.Queue, err error) {
	queue, err = c.Channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return queue, err
	}

	err = c.Channel.QueueBind(
		queue.Name, // name of the queue
		"",        // bindingKey
		exchangeName,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return queue, fmt.Errorf("Queue Bind: %s", err)
	}

	return queue, nil
}
