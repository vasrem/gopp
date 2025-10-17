package gopp

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// ParallelProcessor enables parallel processing of items given a specific function.
type ParallelProcessor[T any] struct {
	// Channel that holds the submissions to be processed
	submissionsToBeProcessedCh chan *submission[T]
	// Channel that holds failed submissions that are candidates for retry
	submissionsToBeEvaluatedForRetryCh chan *failedSubmission[T]
	// Channel that holds the submissions that have been completed and need to be finalized
	submissionsToBeFinalizedCh chan *submission[T]
	// Channel that holds failed submissions that won't be retried
	submissionsFailedCh chan *failedSubmission[T]

	// Boolean that indicates whether the processor is started
	started bool
	// Boolean that indicates whether the processor is terminated
	terminated bool

	// Semaphore that controls the number of items that are actively handled by the processor end to end
	submissionsSemaphore *semaphore.Weighted

	// WaitGroup that is used to handle the grace termination of the goroutines that are spawned
	wg *sync.WaitGroup

	options ParallelProcessorOptions

	processFunc func(item *T) error
}

type ParallelProcessorOptions struct {
	bufferSize      int
	numOfWorkers    int
	maxRetries      int
	discardFailures bool
	retryStrategy   RetryStrategy
}

type submission[T any] struct {
	item *T
	*submissionMetadata
}

type submissionMetadata struct {
	try       int
	startTime time.Time
}

type failedSubmission[T any] struct {
	submission *submission[T]
	err        error
}

func NewParallelProcessorOptionsWithDefaults() ParallelProcessorOptions {
	return ParallelProcessorOptions{
		bufferSize:      5,
		numOfWorkers:    5,
		maxRetries:      3,
		discardFailures: true,
		retryStrategy:   nil,
	}
}

func (p *ParallelProcessorOptions) SetBufferSize(bufferSize int) error {
	if bufferSize <= 0 {
		return fmt.Errorf("buffer size must be bigger than 0.")
	}
	p.bufferSize = bufferSize
	return nil
}

func (p *ParallelProcessorOptions) SetNumOfWorkers(numOfWorkers int) error {
	if numOfWorkers <= 0 {
		return fmt.Errorf("number of workers must be bigger than 0.")
	}
	p.numOfWorkers = numOfWorkers
	return nil
}

func (p *ParallelProcessorOptions) SetMaxRetries(maxRetries int) error {
	if maxRetries < -1 {
		return fmt.Errorf("max retries must be bigger than -1.")
	}

	if maxRetries == -1 {
		log.Warn("Unlimited retries are enabled.")
	}

	p.maxRetries = maxRetries
	return nil
}

func (p *ParallelProcessorOptions) SetRetryStrategy(rs RetryStrategy) {
	p.retryStrategy = rs
}

func (p *ParallelProcessorOptions) SetDiscardFailures(discardFailures bool) {
	p.discardFailures = discardFailures
}

func NewParallelProcessor[T any](options ParallelProcessorOptions, processFunc func(item *T) error) *ParallelProcessor[T] {
	return &ParallelProcessor[T]{
		submissionsToBeProcessedCh:         make(chan *submission[T], options.bufferSize),
		submissionsToBeEvaluatedForRetryCh: make(chan *failedSubmission[T], options.bufferSize),
		submissionsToBeFinalizedCh:         make(chan *submission[T], options.bufferSize),
		submissionsFailedCh:                make(chan *failedSubmission[T], options.bufferSize),
		submissionsSemaphore:               semaphore.NewWeighted(int64(options.bufferSize)),
		processFunc:                        processFunc,
		// TODO: Ensure that options can't change after creating the parallel processor
		options: options,
		wg:      &sync.WaitGroup{},
	}
}

func (p *ParallelProcessor[T]) Start() error {
	if p.terminated {
		return fmt.Errorf("processor already terminated, create a new one as this one can't be restarted.")
	}
	if p.started {
		return fmt.Errorf("processor already started.")
	}
	p.started = true

	for i := 0; i < p.options.numOfWorkers; i++ {
		p.wg.Add(1)
		go func(i int) {
			defer p.wg.Done()
			p.process(i)
		}(i)
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.startRouter()
	}()

	return nil
}

func (p *ParallelProcessor[T]) Submit(item *T) error {
	if p.terminated {
		return fmt.Errorf("can't submit more items as the processor is not in an active state.")
	}

	s := submission[T]{
		item: item,
		submissionMetadata: &submissionMetadata{
			startTime: time.Now(),
		},
	}

	p.submissionsSemaphore.Acquire(context.Background(), 1)
	p.submissionsToBeProcessedCh <- &s
	return nil
}

func (p *ParallelProcessor[T]) GetFailedSubmission() (T, error) {
	var out T
	if p.options.discardFailures {
		return out, fmt.Errorf("can't get failed submissions because the processor is set to discard those.")
	}

	failedItem, ok := <-p.submissionsFailedCh
	if !ok {
		return out, fmt.Errorf("there will not be more failed items in the queue")
	}
	p.submissionsToBeFinalizedCh <- failedItem.submission
	return *failedItem.submission.item, nil
}

func (p *ParallelProcessor[T]) Terminate() error {
	if p.terminated {
		return fmt.Errorf("already in the process of terminating or terminated.")
	}
	log.Debug("Starting termination.")
	p.terminated = true

	p.submissionsSemaphore.Acquire(context.Background(), int64(p.options.bufferSize))

	close(p.submissionsToBeProcessedCh)
	close(p.submissionsToBeEvaluatedForRetryCh)
	close(p.submissionsToBeFinalizedCh)
	close(p.submissionsFailedCh)
	p.wg.Wait()

	log.Debug("Terminated.")
	return nil
}

func (p *ParallelProcessor[T]) process(id int) {
	log := log.WithField("processor", id)
	log.Debug("Ready to serve.")

	for submission := range p.submissionsToBeProcessedCh {
		if submission.startTime.After(time.Now()) {
			p.submissionsToBeProcessedCh <- submission
			continue
		}

		log.Debugf("Processing: %v", *submission.item)
		err := p.processFunc(submission.item)
		if err != nil {
			p.submissionsToBeEvaluatedForRetryCh <- &failedSubmission[T]{
				submission: submission,
				err:        err,
			}
			continue
		}

		p.submissionsToBeFinalizedCh <- submission
	}

	log.Debug("Terminating")
}

func (p *ParallelProcessor[T]) startRouter() {
	var isRetryChClosed bool
	var isFinalizeChClosed bool
	for !(isRetryChClosed && isFinalizeChClosed) {
		select {
		case failedSubmission, ok := <-p.submissionsToBeEvaluatedForRetryCh:
			if !ok {
				isRetryChClosed = true
				break
			}
			p.handleRetries(failedSubmission)
		case completedSubmission, ok := <-p.submissionsToBeFinalizedCh:
			if !ok {
				isFinalizeChClosed = true
				break
			}
			p.handleSubmissionFinalization(completedSubmission)
		}
	}
}

func (p *ParallelProcessor[T]) handleRetries(fs *failedSubmission[T]) {
	if fs.submission.try == p.options.maxRetries {
		log.Debugf("Item reached MaxRetries, skipping: %v", *fs.submission.item)
		if p.options.discardFailures {
			p.submissionsToBeFinalizedCh <- fs.submission
			return
		}

		p.submissionsFailedCh <- fs
		return
	}

	newTime := fs.submission.startTime
	if p.options.retryStrategy != nil {
		newTime = p.options.retryStrategy.calculateStartTime(fs.submission.submissionMetadata)
	}

	log.Debugf("Error detected %v and will retry in %v", *fs.submission.item, newTime.Sub(fs.submission.startTime))
	fs.submission.startTime = newTime
	fs.submission.try++

	p.submissionsToBeProcessedCh <- fs.submission
}

func (p *ParallelProcessor[T]) handleSubmissionFinalization(completedSubmission *submission[T]) {
	p.submissionsSemaphore.Release(1)
	if p.options.retryStrategy != nil {
		p.options.retryStrategy.finalizeSubmission(completedSubmission.submissionMetadata)
	}
}
