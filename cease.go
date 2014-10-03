package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"strings"

	"github.com/streadway/amqp"
)

type RadiodanCommand struct {
	Action        string
	CorrelationId string
}

var dryRun bool

func main() {
	host, port := parseArgs()
	listenForCommand(host, port)
}

func parseArgs() (host string, port int) {
	flag.StringVar(&host, "host", "localhost", "Hostname for RabbitMQ")
	flag.IntVar(&port, "port", 5672, "Port for RabbitMQ")
	flag.BoolVar(&dryRun, "dry-run", false, "Dry Run (do not execute command)")

	flag.Parse()

	return
}

func listenForCommand(host string, port int) {
	amqpUri := fmt.Sprintf("amqp://%s:%d", host, port)
	exchangeName := "radiodan"
	routingKey := "command.device.shutdown"

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

	log.Println("[*] Consuming", q.Name)

	forever := make(chan bool)

	go func() {
		for m := range msgs {
			processMessage(m)
		}
	}()

	log.Printf("[*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

func processMessage(msg amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("[!] Msg processing failed:", r)
		}
	}()

	cmd := RadiodanCommand{}

	err := json.Unmarshal(msg.Body, &cmd)
	failOnError(err, "Malformed Radiodan Command")

	log.Printf("[x] Received Action: %s", cmd.Action)

	execCmd(cmd)
}

func execCmd(cmd RadiodanCommand) {
	var shutdownFlag, path string
	var args []string

	if cmd.Action == "shutdown" {
		shutdownFlag = "-h"
	} else {
		shutdownFlag = "-r"
	}

	if dryRun {
		path = "/bin/echo"
		args = []string{"shutdown", path, shutdownFlag, "now"}
	} else {
		path = "/sbin/shutdown"
		args = []string{path, shutdownFlag, "now"}
	}

	shutdown := exec.Cmd{
		Path: path,
		Args: args,
	}

	output, err := shutdown.CombinedOutput()
	outputStr := strings.TrimRight(string(output), "\n")

	failOnError(err, "Could not exec shutdown")
	log.Println("[x] exec:", outputStr)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
