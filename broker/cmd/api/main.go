package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const webPort = "80"

type config struct {
	Rabbit *amqp.Connection
}

func main() {
	rabbitCon, err := connect()

	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	defer rabbitCon.Close()

	app := config{
		Rabbit: rabbitCon,
	}

	log.Printf("Starting broker server on port %s", webPort)

	// define http server
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", webPort),
		Handler: app.routes(),
	}

	// start the server
	err = srv.ListenAndServe()
	if err != nil {
		log.Panic(err)
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
