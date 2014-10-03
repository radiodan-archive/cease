package main

import (
	"flag"
	"fmt"
	"log"
	"os/exec"

	"github.com/streadway/amqp"
)

func main() {
	host, port := parseArgs()
	listenForCommand(host, port)
}

func parseArgs() (host string, port int) {
	flag.StringVar(&host, "host", "localhost", "Hostname for RabbitMQ")
	flag.IntVar(&port, "port", 5672, "Port for RabbitMQ")

	flag.Parse()

	return
}

func listenForCommand(host string, port int) {
	amqpUri := fmt.Sprintf("amqp://%s:%d", host, port)
	//amqpUri := fmt.Sprintf("amqp://%s", host)
	exchangeName := "radiodan"
	//routingKey := "radiodan.system.shutdown"
	routingKey := "command.*.*"

	conn, err := amqp.Dial(amqpUri)
	failOnError(err, "Cannot connect")
	defer conn.Close()

	log.Printf("[*] Connected to %s", amqpUri)

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	fmt.Println("queue name", q.Name)
	err = ch.QueueBind(
		q.Name,     // queue name
		routingKey, // routing key
		"radiodan", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

func execCmd(shutdownType string) {
	var shutdownFlag string

	if shutdownType == "shutdown" {
		shutdownFlag = "-h"
	} else {
		shutdownFlag = "-r"
	}

	shutdown := exec.Cmd{
		Path: "/sbin/shutdown",
		Args: []string{"/sbin/shutdown", shutdownFlag, "now"},
	}

	output, err := shutdown.CombinedOutput()

	failOnError(err, "Could not exec shutdown")

	log.Println("exec:", output)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
