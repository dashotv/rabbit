package rabbit

import (
	"encoding/json"
)

type Message struct {
	Name string
	Data map[string]string //TODO: figure this out
}

type Messenger interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

func NewMessage(name string) *Message {
	return &Message{Name: name, Data: make(map[string]string)}
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
