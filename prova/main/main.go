package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	godotenv "github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
)

var connectHandler MQTT.OnConnectHandler = func(client MQTT.Client) {
	fmt.Println("Connected")
}

var connectLostHandler MQTT.ConnectionLostHandler = func(client MQTT.Client, err error) {
	fmt.Printf("Connection lost: %v", err)
}

var MessageHandler MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
	KafkaMessage(string(msg.Payload()))
}

func main() {

	username := os.Getenv("USERNAME")
	password := os.Getenv("PASSWORD")
	if username == "" || password == "" {
		// GitHub Secrets not found, try loading from .env file
		err := godotenv.Load("../.env")
		if err != nil {
			fmt.Println("Error loading .env file")
			return
		}

		username = os.Getenv("HIVE_USER")
		password = os.Getenv("HIVE_PSWD")
	}
	var broker = os.Getenv("BROKER_ADDR")
	var port = 8883
	opts := MQTT.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tls://%s:%d", broker, port))
	opts.SetClientID("prova2")
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetDefaultPublishHandler(MessageHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	Subscribe("botafogo", client, MessageHandler)
	Publish(client, 60)

	client.Disconnect(300000)
}

type Payload struct {
	IdSensor     string  `json:"idSensor"`
	Timestamp    string  `json:"timestamp"`
	TipoPoluente string  `json:"tipoPoluente"`
	Nivel        float64 `json:"nivel"`
}

func (s *Payload) ToJSON() (string, error) {
	jsonData, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

func CreatePayload() string {
	id_sensor := "001_sensor"
	timestamp := "2024-04-04T12:34:56Z"
	tipo_poluente := "PM2.5"
	nivel := 35.2

	content := Payload{
		IdSensor:     id_sensor,
		Timestamp:    timestamp,
		TipoPoluente: tipo_poluente,
		Nivel:        nivel,
	}

	payload, _ := content.ToJSON()
	return payload
}

func ToStruct(jsonStr string, msg *kafka.Message) {
	data := Payload{}

	err := json.Unmarshal([]byte(jsonStr), &data)

	if err != nil {
		log.Println(err)
		return
	}

	fmt.Printf("\nId do Sensor: %s", data.IdSensor)
	fmt.Printf("\nTipo de poluente: %s", data.TipoPoluente)
	fmt.Printf("\nHorario: %s", data.Timestamp)
	fmt.Printf("\nNivel de poluição %f", data.Nivel)

}

func Publish(client MQTT.Client, repTime time.Duration) {

	topic := "botafogo"

	token := client.Publish(topic, 1, false, CreatePayload())

	token.Wait()

	if token.Error() != nil {
		fmt.Printf("Failed to publish to topic: %s", topic)
		panic(token.Error())
	}
	time.Sleep(repTime * time.Second)
}

func Subscribe(topic string, client MQTT.Client, handler MQTT.MessageHandler) {
	token := client.Subscribe(topic, 1, handler)
	token.Wait()
	if token.Error() != nil {
		fmt.Printf("Failed to subscribe to topic: %v", token.Error())
		panic(token.Error())
	}
	fmt.Printf("\nSubscribed to topic: %s\n", topic)
}

func KafkaMessage(message string) string {

	// Configurações do produtor
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"client.id":         "go-producer",
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	// Enviar mensagem
	topic := "test_topic"
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, nil)

	// Aguardar a entrega de todas as mensagens
	producer.Flush(15 * 1000)

	// Configurações do consumidor
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"group.id":          "go-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// Assinar tópico
	consumer.SubscribeTopics([]string{topic}, nil)

	// Consumir mensagens
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			ToStruct(string(msg.Value), msg)
			return string(msg.Value)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}
	return "{}"
}
