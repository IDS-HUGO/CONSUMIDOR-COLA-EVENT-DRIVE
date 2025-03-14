package main

import (
    "fmt"
    "log"
    "os"

    MQTT "github.com/eclipse/paho.mqtt.golang"
    "github.com/joho/godotenv"
)

func messageHandler(client MQTT.Client, msg MQTT.Message) {
    log.Printf("Received message: %s from topic: %s", msg.Payload(), msg.Topic())
}

func main() {
    err := godotenv.Load()
    if err != nil {
        log.Println("No se pudo cargar el archivo .env, usando variables del sistema")
    }

    broker := os.Getenv("RABBITMQ_URL")
    topic := os.Getenv("RABBITMQ_QUEUE_IN")

    opts := MQTT.NewClientOptions()
    opts.AddBroker(broker)
    opts.SetClientID("COLAEVENTDRIVE")
    opts.SetDefaultPublishHandler(messageHandler)

    client := MQTT.NewClient(opts)
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        log.Fatalf("Failed to connect to MQTT broker: %v", token.Error())
    }
    defer client.Disconnect(250)

    if token := client.Subscribe(topic, 1, messageHandler); token.Wait() && token.Error() != nil {
        log.Fatalf("Failed to subscribe to topic: %v", token.Error())
    }

    fmt.Println(" [*] Waiting for messages. To exit press CTRL+C")
    select {}
}