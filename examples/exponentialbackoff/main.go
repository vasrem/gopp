// This example runs a producer that submits random strings until a SIGINT signal is caught. The parallel producer uses
// exponential backoff retry strategy and retries each object up to 3 times which is the default. If the retries are
// exceeded and the submission didn't succeed, the failed item is discarded.
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/vasrem/gopp"
)

const NumOfWorkers = 10
const BufferSize = 100

func main() {
	log.SetLevel(log.DebugLevel)

	pp := setupParallelProcessor()

	err := pp.Start()
	if err != nil {
		log.Fatal(err)
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go produce(ctx, &wg, pp)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	<-signalChan

	ctxCancel()
	wg.Wait()

	err = pp.Terminate()
	if err != nil {
		log.Fatal(err)
	}
}

func setupParallelProcessor() *gopp.ParallelProcessor[string] {
	o := gopp.NewParallelProcessorOptionsWithDefaults()
	o.SetBufferSize(BufferSize)
	o.SetNumOfWorkers(NumOfWorkers)
	o.SetRetryStrategy(gopp.NewRetryExponentialBackoff())

	pp := gopp.NewParallelProcessor[string](o, func(obj *string) error {
		r := rand.Float64()
		log.Print(*obj)
		time.Sleep(time.Duration(r*1000) * time.Millisecond)
		if r > 0.9 {
			return fmt.Errorf("too bad that I errored out.")
		}
		return nil
	})

	return pp
}

func produce(ctx context.Context, wg *sync.WaitGroup, pp *gopp.ParallelProcessor[string]) {
	for {
		select {
		case <-ctx.Done():
			log.WithField("component", "producer").Info("Stop producing because context is cancelled")
			wg.Done()
			return
		default:
			s := RandString(50)
			err := pp.Submit(&s)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func RandString(n int) string {
	b := make([]rune, n)
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
