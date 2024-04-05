package main

import (
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	godotenv "github.com/joho/godotenv"
	"os"
	"testing"
	"time"
)


func TestMain(t *testing.T) {
	broker := os.Getenv("BROKER_ADDR")
	username := os.Getenv("HIVE_USER")
	password := os.Getenv("HIVE_PSWD")

	if username == "" || password == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			fmt.Printf("\nError loading .env file. error: %s\n", err)
		}
		broker = os.Getenv("BROKER_ADDR")
		username = os.Getenv("HIVE_USER")
		password = os.Getenv("HIVE_PSWD")
	}
	var port = 8883
	opts := MQTT.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tls://%s:%d", broker, port))
	opts.SetClientID("prova2")
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	client := MQTT.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	t.Run("TestMessageReceivedMatches", func(t *testing.T) {
		var messageReceive MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {

			if  KafkaMessage(string(msg.Payload())) != CreatePayload(){
				t.Error("Message received does not match message sent")
			} else {
				fmt.Print("It did work")
			}
		}
		opts.SetDefaultPublishHandler(MessageHandler)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}

		if token := client.Subscribe("botafogo", 1, messageReceive); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
			return
		}

		Publish(client, 2)

		time.Sleep(2 * time.Second)
		client.Disconnect(250)

	})
}


