package main

import (
	"flag"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	zmq "github.com/pebbe/zmq4"
)

const (
	HEARTBEAT_LIVENESS = 3
	HEARTBEAT_INTERVAL = 2500 * time.Millisecond
	HEARTBEAT_EXPIRY   = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
	PROTOCOL_CLIENT    = "MDPC02"
	PROTOCOL_WORKER    = "MDPW02"
)

const (
	_ = string(int(iota))
	COMMAND_READY
	COMMAND_REQUEST
	COMMAND_REPLY
	COMMAND_HEARTBEAT
	COMMAND_DISCONNECT
)

var dryRun bool

func main() {
	host, port := parseArgs()
	listenForCommand(host, port)
}

func parseArgs() (host string, port int) {
	flag.StringVar(&host, "host", "localhost", "Hostname for broker")
	flag.IntVar(&port, "port", 7171, "Port for RPC on broker")
	flag.BoolVar(&dryRun, "dry-run", false, "Dry Run (do not execute command)")

	flag.Parse()

	return
}

func readySocket(socket *zmq.Socket) {
	log.Println("[*] Registering service device.shutdown")
	socket.SendMessage(PROTOCOL_WORKER, COMMAND_READY, "device.shutdown")
}

func listenForCommand(host string, port int) {
	brokerURL := fmt.Sprintf("tcp://%s:%d", host, port)

	context, err := zmq.NewContext()
	failOnError(err, "Cannot create context")

	socket, err := context.NewSocket(zmq.DEALER)
	failOnError(err, "Cannot create socket")

	socket.SetIdentity("radiodan-cease")

	err = socket.Connect(brokerURL)
	failOnError(err, "Cannot connect to broker")

	defer socket.Close()

	readySocket(socket)

	// TODO: send message registering services

	log.Println("[*] Consuming")

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)

	for {
		var polled []zmq.Polled

		polled, err = poller.Poll(HEARTBEAT_INTERVAL)

		if err != nil {
			break //  Interrupted
		}

		if len(polled) > 0 {
			msg, err := socket.RecvMessage(0)
			if err != nil {
				break //  Interrupted
			}

			log.Printf("I: received message from broker: %q\n", msg)

			switch msg[0] {
			case COMMAND_REQUEST:
				log.Println("REQUEST")

				if len(msg) < 6 {
					log.Printf("!: len(msg) < 6")
					continue
				}

				sender := msg[1]
				correlationId := msg[2]
				serviceType := msg[3]
				serviceInstance := msg[4]
				cmd := msg[5]

				if serviceType != "device" || serviceInstance != "shutdown" {
					log.Printf("!: Invalid service %s.%s", serviceType, serviceInstance)
					continue
				}

				isValid := (cmd == "restart" || cmd == "shutdown")

				if isValid == true {
					execCmd(cmd)
					socket.SendMessage(
						PROTOCOL_WORKER, COMMAND_REQUEST, sender, correlationId,
					)
				} else {
					errMsg, _ := fmt.Printf("!: Invalid command %s", cmd)
					log.Println(errMsg)
					socket.SendMessage(
						PROTOCOL_WORKER, COMMAND_REQUEST, sender, correlationId, errMsg,
					)
				}
			case COMMAND_HEARTBEAT:
				log.Println("HEARTBEAT")
				//socket.SendMessage(PROTOCOL_WORKER, COMMAND_HEARTBEAT)
			case COMMAND_DISCONNECT:
				// Attempt to reconnect
				log.Println("DISCONNECT")
				readySocket(socket)
			default:
				log.Printf("E: invalid input message %q\n", msg)
			}
		}
	}
}

func execCmd(action string) {
	var shutdownFlag, path string
	var args []string

	switch action {
	case "restart":
		shutdownFlag = "-r"
	case "shutdown":
		shutdownFlag = "-h"
	default:
		panic("Action " + action + " is neither restart nor shutdown")
	}

	if dryRun {
		path = "/bin/echo"
		args = []string{path, "shutdown", shutdownFlag, "now"}
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
