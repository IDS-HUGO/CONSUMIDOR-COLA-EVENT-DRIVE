package main

import (
	"bytes"
	"log"
	"net/http"
	"os"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
)

func sendToAPI(message string) {
	apiURL := os.Getenv("API_URL")
	if apiURL == "" {
		log.Println("❌ API_URL no está configurada en el .env")
		return
	}

	if message == "" {
		log.Println("⚠️ Mensaje vacío, no se enviará a la API")
		return
	}

	log.Printf("📤 Enviando mensaje a la API: %s", message)

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer([]byte(message)))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ Error enviando mensaje a la API: %v", err)
		return
	}
	defer resp.Body.Close()

	log.Printf("✅ Mensaje enviado a la API con éxito: %s", message)
}

func messageHandler(client MQTT.Client, msg MQTT.Message) {
	payload := string(msg.Payload())

	log.Printf("📩 Mensaje recibido: '%s' desde el tópico: '%s'", payload, msg.Topic())

	if payload == "" {
		log.Println("⚠️ Advertencia: Se recibió un mensaje vacío desde MQTT")
		return
	}

	sendToAPI(payload)
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("⚠️ No se pudo cargar el archivo .env, usando variables del sistema")
	}

	broker := os.Getenv("RABBITMQ_URL")
	topic := os.Getenv("RABBITMQ_QUEUE_IN")

	if broker == "" || topic == "" {
		log.Fatal("❌ ERROR: RABBITMQ_URL o RABBITMQ_QUEUE_IN no están configurados en el .env")
	}

	opts := MQTT.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID("COLAEVENTDRIVE")
	opts.SetDefaultPublishHandler(messageHandler)

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("❌ Error al conectar con el broker MQTT: %v", token.Error())
	}
	defer client.Disconnect(250)

	if token := client.Subscribe(topic, 1, messageHandler); token.Wait() && token.Error() != nil {
		log.Fatalf("❌ Error al suscribirse al tópico: %v", token.Error())
	}

	log.Println(" [*] ✅ Esperando mensajes en MQTT. Presiona CTRL+C para salir.")
	select {}
}
