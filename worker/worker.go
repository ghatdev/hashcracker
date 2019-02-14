package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/mattheath/base62"
	sarama "gopkg.in/Shopify/sarama.v1"
)

type msg struct {
	Target string
	Salt   string
	Start  int64
	End    int64
	wg     *sync.WaitGroup
}

type worker struct {
	found chan string
	m     chan *msg
}

var workerPool []worker

func (w *worker) Work() {
	for {
		m := <-w.m
		var n int64

	WorkLoop:
		for n := m.Start; n < m.End; n++ {
			select {
			case t := <-w.found: // when cancel message received
				if t == m.Target {
					m.wg.Done()
					break WorkLoop
				}
			default: // or work
				if m.Target == generateHashString(m.Salt, n) {
					fmt.Printf("Found hash! - %v\n", base62.EncodeInt64(n))
					cancelWorks(m.Target)
				}
			}
		}

		if n >= m.End {
			m.wg.Done()
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
	for _, w := range workerPool {
		w.CancelWorking(target)
	}
}

func createPool() {
	nCore := runtime.NumCPU()

	for i := 0; i < nCore; i++ {
		w := worker{found: make(chan string, 1), m: make(chan *msg, 1)}
		workerPool = append(workerPool, w)
		go workerPool[i].Work()
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
			t := crackTarget(m.Target, m.Salt, m.Start, m.End)
			log.Printf("\nTarget cracked - elapsed time: %v\n", t)
		}
	}

	return nil
}

func crackTarget(target, salt string, start, end int64) time.Duration {
	nCore := runtime.NumCPU()
	startTime := time.Now()

	wg := sync.WaitGroup{}

	for i, w := range workerPool {
		wg.Add(1)
		w.GetTarget(&msg{
			Target: target,
			Salt:   salt,
			Start:  start * int64(i) / int64(nCore),
			End:    end * int64(i+1) / int64(nCore),
			wg:     &wg,
		})
	}

	wg.Wait()

	return time.Since(startTime)
}

func generateHashString(salt string, n int64) string {
	str := salt + base62.EncodeInt64(n)

	hash := sha256.Sum256([]byte(str))

	return hex.EncodeToString(hash[:])
}
