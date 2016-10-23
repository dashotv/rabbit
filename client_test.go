package rabbit

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	var client *Client
	var err error
	var consuming chan []byte
	var publishing chan []byte
	done := make(chan int, 1)

	url := "amqp://guest:guest@localhost:5672/"
	exchangeName := "dashotv.testing"
	exchangeType := "fanout"
	queueName := "dashotv.testing.client"

	if client, err = NewClient(url); err != nil {
		fmt.Println("error: ", err)
		return
	}

	if consuming, err = client.Consumer(exchangeName, exchangeType, queueName); err != nil {
		fmt.Println("error: ", err)
		return
	}

	if publishing, err = client.Producer(exchangeName, exchangeType); err != nil {
		fmt.Println("error: ", err)
		return
	}

	s := fmt.Sprintf("timer: %s", time.Now())
	p := NewMessage("dashotv.testing.timer")
	p.Data["message"] = s

	go func() {
		var m []byte
		var e error
		//fmt.Println(s)
		if m, e = json.Marshal(p); e != nil {
			t.Error("marshalling: ", e)
		}
		//fmt.Println("sending: ",string(m))
		publishing <- m
		//fmt.Println("publishing finished")
	}()

	go func() {
		select {
		case n := <-consuming:
			//fmt.Println("receiving: ", string(n))
			q := &Message{}
			if err = json.Unmarshal(n, q); err != nil {
				t.Error("unmarshaling: ", err)
			}
			if q.Name != p.Name {
				t.Error("names do not match")
			}
		}
		//fmt.Println("consuming finished")
		done <- 1
	}()

	timer := time.After(time.Second * 5)
	select {
	case <-done:
		//fmt.Println("done")
	case <-timer:
		t.Error("Timed out")
	}
}
