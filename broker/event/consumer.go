package event

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	conn      *amqp.Connection
	queueName string
}

type Payload struct {
	Name string `json:"name"`
	Data string `json:"data"`
}

func NewConsumer(conn *amqp.Connection) (Consumer, error) {
	consumer := Consumer{
		conn: conn,
	}

	err := consumer.setup()
	if err != nil {
		return Consumer{}, err
	}

	return consumer, nil
}

func (con *Consumer) setup() error {
	// made channel
	channel, err := con.conn.Channel()
	if err != nil {
		return err
	}

	// declare exchange
	return declareExchange(channel)
}

func (c *Consumer) Listen(topics []string) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}

	defer ch.Close()

	// get a random queue
	q, err := declareRandQueue(ch)
	if err != nil {
		return err
	}

	for _, s := range topics {
		// bind topic to queue
		err = ch.QueueBind(
			q.Name,
			s,
			"logs_topic",
			false,
			nil,
		)

		if err != nil {
			return err
		}
	}

	messages, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	forever := make(chan bool)
	go func() {
		for d := range messages {
			var payload Payload
			_ = json.Unmarshal(d.Body, &payload)
			go handlePayload(payload)
		}
	}()

	log.Printf("Waiting for messages on [Exchange, Queue] [logs_topic, %s] \n", q.Name)
	<-forever

	return nil
}

func handlePayload(payload Payload) {
	switch payload.Name {
	case "log", "event":
		//log whatever we get
		err := logEvent(payload)
		if err != nil {
			log.Println(err)
		}

	case "auth":
		//authenticate

		// Make your own cases
	default:
		err := logEvent(payload)
		if err != nil {
			log.Println(err)
		}
	}
}

func logEvent(entry Payload) error {
	jd, _ := json.MarshalIndent(entry, "", "\t")
	logServiceurl := "http://logger-service/log"
	req, err := http.NewRequest("POST", logServiceurl, bytes.NewBuffer(jd))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		return err
	}

	return nil
}
