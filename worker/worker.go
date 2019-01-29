package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/mattheath/base62"
	sarama "gopkg.in/Shopify/sarama.v1"
)

type msg struct {
	Target string
	Salt   string
	Start  int64
	End    int64
}

func main() {
	consumer, err := sarama.NewConsumer([]string{
		"localhost:9092",
	}, nil)

	if err != nil {
		panic(err)
	}

	partitions, err := consumer.Partitions("Hashs")
	if err != nil {
		panic(err)
	}

	initialOffset := sarama.OffsetOldest

	for _, p := range partitions {
		pc, err := consumer.ConsumePartition("Hashs", p, initialOffset)
		if err != nil {
			log.Println(err)
			continue
		}

		for message := range pc.Messages() {
			m := &msg{}
			err := json.Unmarshal([]byte(message.Value), m)
			if err != nil {
				log.Println(err)
				continue
			}

			log.Printf("\nTarget received: %v\nRange: %v~%v\nSalt: %v\n", m.Target, m.Start, m.End, m.Salt)
			calcHash(m.Target, m.Salt, m.Start, m.End)
		}
	}
}

func calcHash(target, salt string, start, end int64) {
	wg := &sync.WaitGroup{}
	nCore := runtime.NumCPU()

	for i := 0; i < nCore; i++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for n := start * int64(c) / int64(nCore); n < end*int64(c+1)/int64(nCore); n++ {
				if target == shaBase62(salt, n) {
					// now just stop
					fmt.Printf("Found hash! - %v\n", base62.EncodeInt64(n))
					os.Exit(0)
				}
			}

		}(i)
	}

	wg.Wait()
}

func shaBase62(salt string, n int64) string {
	str := salt + base62.EncodeInt64(n)

	hash := sha256.Sum256([]byte(str))

	return hex.EncodeToString(hash[:])
}
