package rabbit

import (
	"github.com/streadway/amqp"
	"encoding/json"
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

//func (c *Client) Connect(exchangeType string, exchangeName string) error {
//	c.Exchange = exchangeName
//
//	cx, err := amqp.Dial(c.Url)
//	if err != nil {
//		return err
//	}
//	c.Connection = cx
//
//	channel, err := cx.Channel()
//	if err != nil {
//		return err
//	}
//	c.Channel = channel
//
//	err = channel.ExchangeDeclare(
//		exchangeName, // name
//		exchangeType, // type
//		true,         // durable
//		false,        // auto-deleted
//		false,        // internal
//		false,        // noWait
//		nil,          // arguments
//	)
//	if err != nil {
//		return err
//	}
//
//	c.Connected = true
//	return nil
//}

func (c *Client) Publisher(exchangeName string, exchangeType string, encode func(interface{}) []byte) (chan interface{}, error) {
	publishing := make(chan interface{}, 1)

	if err := c.exchange(exchangeName, exchangeType); err != nil {
		return nil, err
	}

	// add concurrency
	// loop to make more goroutines based on int arg
	go func() {
		for msg := range publishing {
			//var data []byte
			var err error

			//if data, err = msg.Marshal(); err != nil {
			//	fmt.Println("error marshaling: ", err)
			//}

			if err = c.publish(exchangeName, encode(msg)); err != nil {
				fmt.Println("error publishing: ", err)
				return
			}
		}
	}()

	return publishing, nil
}

func (c *Client) Consumer(exchangeName string, exchangeType string, queueName string, decode func([]byte) interface{}) (chan interface{}, error) {
	consuming := make(chan interface{}, 1)
	var queue amqp.Queue
	var err error

	if err = c.exchange(exchangeName, exchangeType); err != nil {
		return nil, err
	}

	if queue, err = c.queue(exchangeName, exchangeType, queueName); err != nil {
		return nil, err
	}

	go func() {
		deliveries, err := c.Channel.Consume(
			queue.Name, // name
			"",      // consumerTag,
			false,      // noAck
			false,      // exclusive
			false,      // noLocal
			false,      // noWait
			nil,        // arguments
		)
		if err != nil {
			fmt.Println("error consuming: ", err)
			return
		}

		for raw := range deliveries {
			consuming <- decode(raw.Body)
		}
	}()

	return consuming, nil
}

//func (c *Client) Publish(message string) error {
//	err := c.Channel.Publish(
//		c.Exchange, // publish to an exchange
//		"", // routing to 0 or more queues
//		false, // mandatory
//		false, // immediate
//		amqp.Publishing{
//			Headers:         amqp.Table{},
//			ContentType:     "text/plain",
//			ContentEncoding: "",
//			Body:            []byte(message),
//			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
//			Priority:        0, // 0-9
//			// a bunch of application/implementation-specific fields
//		},
//	)
//	if err != nil {
//		return err
//	}
//	return nil
//}

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

func (c *Client) publish(exchangeName string, msg interface{}) error {
	var data []byte
	var err error

	if data, err = json.Marshal(&msg); err != nil {
		return fmt.Errorf("error marshaling: %s", err)
	}
	return c.Channel.Publish(
		exchangeName, // publish to an exchange
		"", // routing to 0 or more queues
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "utf8",
			Body:            data,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0, // 0-9
			// a bunch of application/implementation-specific fields
		},
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
