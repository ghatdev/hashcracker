package main

import (
	"encoding/json"
	"log"
	"math"
	"strings"

	sarama "gopkg.in/Shopify/sarama.v1"
)

type msg struct {
	Target string
	Salt   string
	Start  int64
	End    int64
}

func main() {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	c, err := sarama.NewSyncProducer([]string{
		"localhost:9092",
	}, config)
	if err != nil {
		panic(err)
	}

	defer c.Close()

	target := strings.ToLower("6B58896A74C09A95D1D56FE51BD90D0AAB6187258D6BC446DC989947E7426811")
	salt := ""
	l := 5

	total := int64(math.Pow(62, float64(l)))

	start := int64(0)
	end := total / int64(math.Pow(62, 4))

	for i := start; i < end; i++ {
		m := &msg{
			Target: target,
			Salt:   salt,
			Start:  i * int64(math.Pow(62, 4)),
			End:    (i + 1) * int64(math.Pow(62, 4)),
		}
		message, err := json.Marshal(m)
		if err != nil {
			log.Println(err)
			continue
		}

		p, offset, err := c.SendMessage(&sarama.ProducerMessage{
			Topic:     "Hashs",
			Partition: 1,
			Value:     sarama.ByteEncoder(message),
		})
		if err != nil {
			log.Println(err)
			continue
		}

		log.Printf("Partition: %v, Offset: %v\nHash: %v\nRange: %v~%v\nSalt:%v\n", p, offset, target, m.Start, m.End, salt)
	}
}
