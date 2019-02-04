package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"runtime"

	sarama "gopkg.in/Shopify/sarama.v1"
	"github.com/mattheath/base62"
)

type msg struct {
	Target string
	Salt   string
	Start  int64
	End    int64
}

type worker struct {
	found chan string
	m     chan *msg
}

var workerPool []worker

func (w *worker) Work() {
	var m msg
	for {
		m = <-w.m
		WorkLoop:
		for n := m.Start; n < m.End; n++ {
			select {
			case t := <-w.found: // when cancel message received
				if t == m.Target {
					break WorkLoop
				}
			default: // or work
				if target == gemerateHashString(salt, n) {
					// now just stop
					fmt.Printf("Found hash! - %v\n", base62.EncodeInt64(n))
					cancelWorks(m.Target)
				}
			}
		}
	}
}

func (w *worker) CancelWorking(target string) {
	w.found <- target
}

func (w *worker) GetTarget(m *msg) {
	w.m <- m
}

func cancelWorks(target string) {
	for _, w := workerPool {
		w.CancelWorking(target)
	}
}

func createPool() {
	nCore := runtime.NumCPU()

	foundPipe := make(chan string, nCore)
	msgPipe := make(chan msg, nCore)

	workerPool = make([]worker, nCore)

	for i := 0; i < nCore; i++ {
		workerPool = append(pool, worker{found: foundPipe, m: msgPipe})
		pool[i].Work()
	}
}

func Work(kafkaServer []string, topic string) error {
	consumer, err := sarama.NewConsumer(kafkaServer, nil)

	if err != nil {
		return err
	}

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return err
	}

	createPool()

	for _, p := range partitions {
		pc, err := consumer.ConsumePartition(topic, p, sarama.OffsetOldest)
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
			broadcastMsg(m.Target, m.Salt, m.Start, m.End)
		}
	}
}

func broadcastMsg(target, salt string, start, end int64) {
	nCore := runtime.NumCPU()

	for i, w := range workerPool {
		w.GetTarget(&msg{
			Target:target,
			Salt: salt,
			Start: start * int64(i) / int64(nCore),
			End: end*int64(i+1)/int64(nCore),
		})
	}
}

func gemerateHashString(salt string, n int64) string {
	str := salt + base62.EncodeInt64(n)

	hash := sha256.Sum256([]byte(str))

	return hex.EncodeToString(hash[:])
}
