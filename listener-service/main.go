package main

import (
	"log"
	"math"
	"os"
	"time"

	"github.com/anveshthakur/listener-service/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// try to connect to rabbitMQ
	rabbitConn, err := connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	defer rabbitConn.Close()

	// start listening to messages
	log.Println("Listening for and consuming messages...")

	// create consumer
	consumer, err := event.NewConsumer(rabbitConn)
	if err != nil {
		panic(err)
	}

	// watch the Queue and consume events
	err = consumer.Listen([]string{"LOG.INFO", "LOG.WARN", "LOG.ERROR"})
	if err != nil {
		log.Println(err)
	}
}

func connect() (*amqp.Connection, error) {
	var count int64
	backOff := 1 * time.Second
	var connection *amqp.Connection

	//Don't continue until mq is ready
	for {
		c, err := amqp.Dial("amqp://guest:guest@rabbitmq")
		if err != nil {
			log.Println("RabbitMQ not ready")
			count++
		} else {
			log.Println("Connected to rabbitMQ")
			connection = c
			break
		}

		if count > 5 {
			log.Println(err)
			return nil, err
		}

		backOff = time.Duration(math.Pow(float64(count), 2)) * time.Second
		log.Println("Backoff...")
		time.Sleep(backOff)
		continue
	}

	return connection, nil
}
