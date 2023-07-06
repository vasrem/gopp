// This example illustrates how one can submit 50 integers into the parallel processor, make it calculate the power of 2
// for each one of them in parallel and terminate gracefully the parallel processor.
package main

import (
	"math"

	log "github.com/sirupsen/logrus"
	"github.com/vasrem/gopp"
)

const numOfSubmissions = 50

func main() {
	options := gopp.NewParallelProcessorOptionsWithDefaults()
	pp := gopp.NewParallelProcessor[int](options, func(obj *int) error {
		log.Printf("The power of 2 for number %d is %d", *obj, int(math.Pow(float64(*obj), 2)))
		return nil
	})

	err := pp.Start()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < numOfSubmissions; i++ {
		s := i
		pp.Submit(&s)
	}

	err = pp.Terminate()
	if err != nil {
		log.Fatal(err)
	}
}
