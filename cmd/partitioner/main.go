package main

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"log"
	"net"
	"net/http"
	"os"
	"path"
)

const InPartitionCount = 1
const InTopic = "msk-metrics-prod"

var InBrokers = []string{}

const OutPartitionCount = 8
const OutTopic = "partitioned_by_metric"

var OutBroker = []string{}

const DefaultLastOffsetFile = "offset/last_offset"

func getLastOffset() (int64, error) {
	lastOffsetFile := getLastOffsetFile()

	f, err := os.Open(lastOffsetFile)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var payload map[string]interface{}
	if err = json.NewDecoder(f).Decode(&payload); err != nil {
		return 0, err
	}
	return int64(payload["offset"].(int64)), nil
}

func writeLastOffset(offset int64) error {
	lastOffsetFile := getLastOffsetFile()

	f, err := os.OpenFile(lastOffsetFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	payload := make(map[string]interface{})
	payload["offset"] = offset
	if err = json.NewEncoder(f).Encode(payload); err != nil {
		return err
	}
	return nil
}

func getLastOffsetFile() string {
	if os.Getenv("LAST_OFFSET_FILE") != "" {
		return os.Getenv("LAST_OFFSET_FILE")
	} else {
		return DefaultLastOffsetFile
	}
}

type Insight struct {
	LastOffsetConsumed    int64 `json:"lastOffsetConsumed"`
	TotalMessagesConsumed int64 `json:"totalMessagesConsumed"`
	TotalMessagesProduced int64 `json:"totalMessagesProduced"`
}

func main() {
	if err := os.MkdirAll(path.Dir(getLastOffsetFile()), 0700); err != nil {
		log.Fatalln("Error creating offset directory: ", err)
	}

	config := buildKafkaConfig()

	consumer, err := sarama.NewConsumer(InBrokers, config)
	if err != nil {
		log.Fatalln("Error creating consumer: ", err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	//offset, err := getLastOffset()
	//if err != nil {
	//	log.Fatalln("Error reading last offset: ", err)
	//}

	partitionConsumer, err := consumer.ConsumePartition(InTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalln("Error creating partition consumer: ", err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	producer, err := sarama.NewAsyncProducer(OutBroker, config)
	if err != nil {
		log.Fatalln("Error creating producer: ", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	producer.Input()

	var insight Insight

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalln("Error creating listener: ", err)
	}
	defer listener.Close()

	go func() {
		log.Fatalln(http.Serve(listener, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(writer).Encode(insight); err != nil {
				log.Println("Error writing http response: ", err)
			}
		})))
	}()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			insight.TotalMessagesConsumed++
			insight.LastOffsetConsumed = msg.Offset
			producerMessage, err := transformMessage(msg)
			if err != nil {
				log.Println("Error reading message: ", err)
			}
			producer.Input() <- producerMessage
			insight.TotalMessagesProduced++
		}
	}
}

func buildKafkaConfig() *sarama.Config {
	// TODO: Finish building the kafka config
	config := sarama.NewConfig()
	config.Net.SASL.Mechanism = "SCRAM-SHA-512"
	return config
}

func transformMessage(msg *sarama.ConsumerMessage) (*sarama.ProducerMessage, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return nil, err
	}
	key := payload["metric"].(string)

	return &sarama.ProducerMessage{
		Topic: OutTopic,
		Value: sarama.ByteEncoder(msg.Value),
		Key:   sarama.StringEncoder(key),
	}, nil
}
