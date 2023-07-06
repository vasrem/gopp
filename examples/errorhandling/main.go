// This example runs a producer that submits random strings until SIGINT is caught. The parallel producer is set to not
// retry on failure and make the errored submissions available through the GetFailedSubmission() method.
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
	pp := setupParallelProcessor()

	err := pp.Start()
	if err != nil {
		log.Fatal(err)
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	var producerWg sync.WaitGroup
	producerWg.Add(1)
	go produce(ctx, &producerWg, pp)

	var errorHandlerWg sync.WaitGroup
	errorHandlerWg.Add(1)
	go handleErrors(&errorHandlerWg, pp)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	<-signalChan

	ctxCancel()
	producerWg.Wait()

	err = pp.Terminate()
	if err != nil {
		log.Fatal(err)
	}

	errorHandlerWg.Wait()
}

func setupParallelProcessor() *gopp.ParallelProcessor[string] {
	o := gopp.NewParallelProcessorOptionsWithDefaults()
	o.SetBufferSize(BufferSize)
	o.SetNumOfWorkers(NumOfWorkers)
	o.SetMaxRetries(0)
	o.SetDiscardFailures(false)

	pp := gopp.NewParallelProcessor[string](o, func(obj *string) error {
		r := rand.Float64()
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

func handleErrors(wg *sync.WaitGroup, pp *gopp.ParallelProcessor[string]) {
	log := log.WithField("component", "errorHandler")
	for {
		i, err := pp.GetFailedSubmission()
		if err != nil {
			log.Infof("Terminating since pp.GetFailedSubmission() errored out with: %s", err.Error())
			break
		}

		log.Infof("Handling item that failed %s", i)
	}

	wg.Done()
}

func RandString(n int) string {
	b := make([]rune, n)
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
