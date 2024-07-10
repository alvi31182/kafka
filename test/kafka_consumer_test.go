package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/alvi31182/kafka"
	"github.com/stretchr/testify/assert"
)

func TestKafkaConsumer(t *testing.T) {
	kfconf, err := kafka.KafkaConfig()
	assert.NoError(t, err)
	fmt.Println(kfconf)

	kc, err := kafka.NewKafkaConsumer(kfconf)
	assert.NoError(t, err)
	fmt.Println(kc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages, err := kc.Consume(ctx, "beta-02_player.v1.account")
	assert.NoError(t, err)
	fmt.Println(messages)
}
