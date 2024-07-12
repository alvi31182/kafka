package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
)

type KafkaConsumer struct {
	kconf *KafkaConfiguration
}

func NewKafkaConsumer(kconf *KafkaConfiguration) (*KafkaConsumer, error) {
	return &KafkaConsumer{kconf: kconf}, nil
}

func (kc KafkaConsumer) Consume(ctx context.Context, topic string) (<-chan *sarama.ConsumerMessage, error) {
	config, err := kc.kconf.KafkaConfig()
	if err != nil {
		fmt.Println(config)
		return nil, err
	}

	// client, err := sarama.NewClient([]string{kc.kconf.DNS}, config)
	// if err != nil {
	// 	return nil, err
	// }

	consumer, err := sarama.NewConsumer([]string{kc.kconf.DNS}, config)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return nil, err
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(kc.kconf.ConsumerGroup)
	if err != nil {
		return nil, err
	}
	defer offsetManager.Close()

	partitionOffsetManager, err := offsetManager.ManagePartition(topic, 0)
	if err != nil {
		return nil, err
	}

	messages := partitionConsumer.Messages()
	go func() {
		var messageID int64 = 0
		for msg := range messages {
			kc.kconf.Logger.Infof("Received message with value: %s", string(msg.Value))

			messageID++

			partitionOffsetManager.MarkOffset(msg.Offset+1, fmt.Sprintf("processed_message_id=%d", messageID))
		}
	}()

	return messages, nil
}
