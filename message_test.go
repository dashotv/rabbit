package rabbit

import (
	"testing"
	"encoding/json"
)

func TestMessage_MarshalSimple(t *testing.T) {
	var m, n *Message
	var err error
	var d []byte

	m = &Message{Name: "simple", Data: "simple message test"}
	if d, err = m.Marshal(); err != nil {
		t.Error(err)
	}

	n = NewMessageFromJson(d)

	if n.Name != m.Name {
		t.Error("names do not match")
	}

	if n.Data != m.Data {
		t.Error("data does not match")
	}
}

type Complex struct {
	Number   int64
	Message  string
	Floating float64
}

type ComplexMessage struct {
	Name string
	Data Complex
	Message
}

func (m *ComplexMessage) Marshal() ([]byte, error) {
	return json.Marshal(*m)
}

func (m *ComplexMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

func TestMessage_MarshalComplex(t *testing.T) {
	var c, e Complex
	var m, n *ComplexMessage
	var err error
	var d []byte

	c = Complex{Number: 10424, Message: "complex message test", Floating: 42.42}
	m = &ComplexMessage{Name: "complex", Data: c}

	if d, err = m.Marshal(); err != nil {
		t.Error(err)
	}

	n = &ComplexMessage{}
	n.Unmarshal(d)

	if n.Name != m.Name {
		t.Error("names do not match")
	}

	e = n.Data
	if c.Number != e.Number {
		t.Error("number does not match")
	}
}
