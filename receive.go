package main

import (
	"bytes"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func sendToAPI(message string) {
	apiURL := os.Getenv("API_URL")
	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer([]byte(message)))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending message to API: %v", err)
		return
	}
	defer resp.Body.Close()

	log.Printf("Message sent to API: %s", message)
}

func main() {
	// Cargar variables de entorno
	err := godotenv.Load()
	if err != nil {
		log.Println("No se pudo cargar el archivo .env, usando variables del sistema")
	}

	rabbitURL := os.Getenv("RABBITMQ_URL")
	queueName := os.Getenv("RABBITMQ_QUEUE_IN")

	// Conectar a RabbitMQ
	conn, err := amqp.Dial(rabbitURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Crear un canal
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declarar la cola
	q, err := ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	// Consumir mensajes de la cola
	msgs, err := ch.Consume(
		q.Name,
		"COLAEVENTDRIVE",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	// Canal para mantener la ejecución
	var forever chan struct{}

	// Goroutine para procesar mensajes
	go func() {
		for d := range msgs {
			log.Printf("Received message: %s", d.Body)
			sendToAPI(string(d.Body))
		}
	}()

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
