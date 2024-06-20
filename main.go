package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

const endpoint = "172.16.10.103:9092"

var topic = "test-topic7"

func main() {
	conn, err := kafka.Dial("tcp", endpoint)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
	}

	// create topic

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}

	httpClient := kafka.Client{}
	resp, err := httpClient.CreatePartitions(context.Background(), &kafka.CreatePartitionsRequest{
		Addr: kafka.TCP(endpoint),
		Topics: []kafka.TopicPartitionsConfig{
			{
				Name:                      topic,
				Count:                     5,
				TopicPartitionAssignments: []kafka.TopicPartitionAssignment{},
			},
		},
	})
	if err != nil {
		panic(err)
	}
	log.Println("receive create partitions response: ", *resp)

	// consume topic

	go func() {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{endpoint},
			Topic:     topic,
			Partition: 0,
			MaxBytes:  10e6, // 10MB
		})

		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				break
			}
			r.CommitMessages(context.Background(), m)
			fmt.Printf("Partition 0 message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		}

		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()

	go func() {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{endpoint},
			Topic:     topic,
			Partition: 1,
			MaxBytes:  10e6, // 10MB
		})

		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				break
			}
			fmt.Printf("Partition 1 message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
			r.CommitMessages(context.Background(), m)
		}

		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()

	go func() {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{endpoint},
			Topic:     topic,
			Partition: 2,
			MaxBytes:  10e6, // 10MB
		})

		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				break
			}
			r.CommitMessages(context.Background(), m)
			fmt.Printf("Partition 2 message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		}

		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()

	// produce message
	w := &kafka.Writer{
		Addr:     kafka.TCP(endpoint),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-D"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-E"),
			Value: []byte("Hello World!"),
		},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

	select {}
}
