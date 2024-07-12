package kafka

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

type KafkaConfiguration struct {
	DNS              string
	ConsumerGroup    string
	KafkaTopicPrefix string
	Logger           *logrus.Logger
}

func KafkaConfig() (*KafkaConfiguration, error) {

	return &KafkaConfiguration{
		DNS:              "affiliate_kafka_go:9092",
		ConsumerGroup:    "affiliate-registration-consumer-group",
		KafkaTopicPrefix: "beta-02_",
		Logger:           logrus.New(),
	}, nil
}

func (kconf *KafkaConfiguration) KafkaConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second
	config.ClientID = "my-client"
	config.Version = sarama.V2_1_0_0
	config.Metadata.Retry.Max = 5
	config.Metadata.Retry.Backoff = 2 * time.Second
	config.Metadata.RefreshFrequency = 10 * time.Minute

	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	config.Consumer.Group.Rebalance.Timeout = 60 * time.Second
	config.Consumer.Group.Rebalance.Retry.Max = 4
	config.Consumer.Group.Rebalance.Retry.Backoff = 2 * time.Second

	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true
	config.Consumer.MaxWaitTime = 10 * time.Second
	config.Consumer.MaxProcessingTime = 10 * time.Second

	return config, nil
}
