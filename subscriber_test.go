package rabbit

import (
	"testing"
	"fmt"
	"time"
	"encoding/json"
)

func TestNewSubscriber(t *testing.T) {
	var client *Client
	var err error
	var sub *Subscriber
	var publishing chan []byte
	done := make(chan int, 1)

	url := "amqp://guest:guest@localhost:5672/"
	exchangeName := "dashotv.testing"
	exchangeType := "fanout"
	queueName := "dashotv.testing.subscriber"

	s := fmt.Sprintf("timer: %s", time.Now())
	p := NewMessage(queueName)
	p.Data["message"] = s

	go func() {
		var m []byte
		var e error

		if client, err = NewClient(url); err != nil {
			t.Error("error: ", err)
		}

		if publishing, err = client.Producer(exchangeName, exchangeType); err != nil {
			fmt.Println("error: ", err)
			return
		}

		//fmt.Println(s)
		if m, e = json.Marshal(p); e != nil {
			t.Error("marshalling: ", e)
		}
		//fmt.Println("sending: ",string(m))
		publishing <- m
		//fmt.Println("publishing finished")
	}()

	go func() {
		var v string
		var ok bool

		if sub, err = NewSubscriber(url, exchangeName, exchangeType, queueName); err != nil {
			t.Error("failed to create subscriber")
		}

		f := func(name string, data map[string]string) {
			if v, ok = data["message"]; !ok {
				t.Error("missing message key")
			}

			if v != p.Data["message"] {
				t.Error("data does not match")
			}

			done <- 1
		}

		//fmt.Println("adding function")
		sub.Add(queueName, f)

		//fmt.Println("subscriber listen")
		sub.Listen()
	}()

	timer := time.After(time.Second * 5)
	select {
	case <-done:
		//fmt.Println("done")
	case <-timer:
		t.Error("Timed out")
	}
}
