package protobus

type Message struct {
	id string
	headers map[string]interface{}
	body interface{}
}

func (m *Message) MessageId() string {
	return m.id
}

func (m *Message) Headers() map[string]interface{} {
	return m.headers
}

func (m *Message) Header(k string) interface{} {
	return m.headers[k]
}

func (m *Message) SetHeaders(h map[string]interface{}) {
	m.headers = h
}

func (m *Message) SetHeader(k string, v interface{}) {
	m.headers[k] = v
}

func (m *Message) Body() interface{} {
	return m.body
}

func (m *Message) SetBody(body interface{}) {
	m.body = body
}