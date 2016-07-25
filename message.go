package rabbit

import (
	"encoding/json"
	"fmt"
)

type Message struct {
	Name string
	Data interface{}
}

type Messenger interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) (error)
}

func NewMessage(name string, object interface{}) *Message {
	fmt.Println("object: ", object)
	return &Message{Name: name, Data: object }
}

func NewMessageFromJson(data []byte) *Message {
	m := &Message{}
	json.Unmarshal(data, m)
	return m
}

func (m *Message) Marshal() ([]byte, error) {
	return json.Marshal(*m)
}

func (m *Message) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}
