package gopp

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TODO Test Cases:
// * semaphore, ensure that X items exist in the whole processor
// * play with combinations of workers and buffer size
// * test the options, for example discard failures

func TestProcessingPresubmittedItems(t *testing.T) {
	numOfItems := 10
	ch := make(chan int, numOfItems+1)
	o := NewParallelProcessorOptionsWithDefaults()
	o.SetBufferSize(numOfItems)
	pp := NewParallelProcessor[int](o, func(item *int) error {
		ch <- *item
		return nil
	})

	for i := 0; i < numOfItems; i++ {
		item := i
		pp.Submit(&item)
	}

	assert.NoError(t, pp.Start())
	assert.NoError(t, pp.Terminate())
	ch <- -1

	var itemAcks int
	for itemAck := range ch {
		if itemAck == -1 {
			break
		}
		itemAcks++
	}

	assert.Equal(t, numOfItems, itemAcks)
	close(ch)
}

func TestProcessingSubmittedItems(t *testing.T) {
	numOfItems := 10
	ch := make(chan int, numOfItems+1)
	o := NewParallelProcessorOptionsWithDefaults()
	pp := NewParallelProcessor[int](o, func(item *int) error {
		ch <- *item
		return nil
	})

	assert.NoError(t, pp.Start())

	for i := 0; i < numOfItems; i++ {
		item := i
		pp.Submit(&item)
	}

	assert.NoError(t, pp.Terminate())
	ch <- -1

	var itemAcks int
	for itemAck := range ch {
		if itemAck == -1 {
			break
		}
		itemAcks++
	}

	assert.Equal(t, numOfItems, itemAcks)
	close(ch)
}

func TestProcessingFailingItems(t *testing.T) {
	numOfItems := 10
	ch := make(chan int, numOfItems+1)
	o := NewParallelProcessorOptionsWithDefaults()
	o.SetMaxRetries(-1)
	pp := NewParallelProcessor[int](o, func(item *int) error {
		r := rand.Float64()
		if r > 0.5 {
			return fmt.Errorf("item failed")
		}
		ch <- *item
		return nil
	})

	assert.NoError(t, pp.Start())

	for i := 0; i < numOfItems; i++ {
		item := i
		pp.Submit(&item)
	}

	assert.NoError(t, pp.Terminate())
	ch <- -1

	var itemAcks int
	for itemAck := range ch {
		if itemAck == -1 {
			break
		}
		itemAcks++
	}

	assert.Equal(t, numOfItems, itemAcks)
	close(ch)
}

func TestGetFailedSubmissions(t *testing.T) {
	numOfItems := 10
	ch := make(chan int, numOfItems)
	o := NewParallelProcessorOptionsWithDefaults()
	o.SetMaxRetries(0)
	o.SetBufferSize(numOfItems)
	o.SetDiscardFailures(false)
	pp := NewParallelProcessor[int](o, func(item *int) error {
		ch <- *item
		return fmt.Errorf("item failed")
	})

	assert.NoError(t, pp.Start())

	for i := 0; i < numOfItems; i++ {
		item := i
		pp.Submit(&item)
	}

	var itemAcks int
	assert.Eventually(t, func() bool {
		defer close(ch)

		for range ch {
			_, err := pp.GetFailedSubmission()
			assert.NoError(t, err)
			itemAcks++

			if itemAcks == numOfItems {
				break
			}
		}

		return true
	}, time.Second*5, 10*time.Millisecond)

	select {
	case <-pp.submissionsToBeProcessedCh:
		assert.Fail(t, "queue should not have any items left")
	case <-pp.submissionsToBeEvaluatedForRetryCh:
		assert.Fail(t, "queue should not have any items left")
	case <-pp.submissionsFailedCh:
		assert.Fail(t, "queue should not have any items left")
	default:
	}

	assert.NoError(t, pp.Terminate())
	_, err := pp.GetFailedSubmission()
	assert.Error(t, err)
}

func TestGetFailedSubmissionWhenItemNotSubmittedButProcessorTerminated(t *testing.T) {
	o := NewParallelProcessorOptionsWithDefaults()
	pp := NewParallelProcessor[int](o, func(item *int) error {
		return fmt.Errorf("item failed")
	})

	assert.NoError(t, pp.Start())

	ch := make(chan struct{}, 1)
	go func() {
		_, err := pp.GetFailedSubmission()
		assert.Error(t, err)
		ch <- struct{}{}
	}()

	assert.NoError(t, pp.Terminate())
	assert.Eventually(t, func() bool {
		<-ch
		close(ch)
		return true
	}, time.Second*5, 10*time.Millisecond)
}

func TestGracefulTermination(t *testing.T) {
	o := NewParallelProcessorOptionsWithDefaults()
	pp := NewParallelProcessor[int](o, func(item *int) error {
		return nil
	})

	assert.NoError(t, pp.Start())
	assert.NoError(t, pp.Terminate())
	assert.Panics(t, func() {
		pp.submissionsToBeProcessedCh <- &submission[int]{}
	})
	assert.Panics(t, func() {
		pp.submissionsToBeEvaluatedForRetryCh <- &failedSubmission[int]{}
	})
	assert.Panics(t, func() {
		pp.submissionsFailedCh <- &failedSubmission[int]{}
	})
	assert.Panics(t, func() {
		pp.submissionsToBeFinalizedCh <- &submission[int]{}
	})
}

func TestSubmittingAfterTermination(t *testing.T) {
	o := NewParallelProcessorOptionsWithDefaults()
	pp := NewParallelProcessor[int](o, func(item *int) error {
		return nil
	})

	assert.NoError(t, pp.Terminate())
	assert.Error(t, pp.Submit(nil))
}

func TestMultipleFunctionCalls(t *testing.T) {
	o := NewParallelProcessorOptionsWithDefaults()
	pp := NewParallelProcessor[int](o, func(item *int) error {
		return nil
	})

	assert.NoError(t, pp.Start())
	assert.Error(t, pp.Start())

	assert.NoError(t, pp.Terminate())
	assert.Error(t, pp.Terminate())
}

func TestTerminateBeforeStarting(t *testing.T) {
	o := NewParallelProcessorOptionsWithDefaults()
	pp := NewParallelProcessor[int](o, func(item *int) error {
		return nil
	})

	assert.NoError(t, pp.Terminate())
	assert.Error(t, pp.Start())
}
