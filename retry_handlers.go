package gopp

import (
	"time"

	backoff "github.com/cenkalti/backoff/v4"
)

type RetryStrategy interface {
	// calculateStartTime should return the time which a submission should be retried
	calculateStartTime(sm *submissionMetadata) time.Time
	// finalizeSubmission is a hook that allows for state cleanup as long as a submission is considered done
	finalizeSubmission(sm *submissionMetadata)
}

type RetryOnInterval struct {
	interval time.Duration
}

var _ RetryStrategy = &RetryOnInterval{}

func NewRetryOnInterval(interval time.Duration) *RetryOnInterval {
	return &RetryOnInterval{
		interval: interval,
	}
}

func (r *RetryOnInterval) calculateStartTime(sm *submissionMetadata) time.Time {
	return sm.startTime.Add(r.interval)
}
func (r *RetryOnInterval) finalizeSubmission(sm *submissionMetadata) {}

type RetryExponentialBackoff struct {
	submissions map[*submissionMetadata]*backoff.ExponentialBackOff
}

var _ RetryStrategy = &RetryExponentialBackoff{}

func NewRetryExponentialBackoff() *RetryExponentialBackoff {
	return &RetryExponentialBackoff{
		submissions: make(map[*submissionMetadata]*backoff.ExponentialBackOff),
	}
}

func (r *RetryExponentialBackoff) calculateStartTime(sm *submissionMetadata) time.Time {
	if v, ok := r.submissions[sm]; ok {
		d := v.NextBackOff()
		return sm.startTime.Add(d)
	}

	eb := backoff.NewExponentialBackOff()
	// MaxRetries are handled in the main logic
	// TODO: Create options struct to give control to the users
	eb.MaxElapsedTime = 0
	r.submissions[sm] = eb
	d := eb.NextBackOff()
	return sm.startTime.Add(d)
}
func (r *RetryExponentialBackoff) finalizeSubmission(sm *submissionMetadata) {
	delete(r.submissions, sm)
}
