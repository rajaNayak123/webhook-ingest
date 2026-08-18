// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// recordingProcessingTimeout bounds background recording processing. It is
// intentionally independent of any HTTP request's lifetime.
const recordingProcessingTimeout = 30 * time.Second

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	// wg tracks background work (currently: recording processing) started
	// by Ingest, so Shutdown can wait for it to finish instead of the
	// process exiting mid-flight.
	wg sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
//
// Deliveries are deduplicated by event_id: the provider delivers at least
// once and may redeliver the same event_id after we already returned 200.
// Ingesting the same event_id twice must not double-count anything, so the
// insert, the call upsert, and the stats increment all happen atomically in
// store.IngestEvent, gated by a unique constraint on event_id. That single
// database-enforced gate is authoritative; the in-memory cache is only
// updated after it confirms this delivery was genuinely new.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the
	// provider. It must not run on the request's context: net/http cancels
	// r.Context() the moment the handler returns, which is long before this
	// goroutine wakes up, so any ctx-bound work after that point is
	// guaranteed to fail. It also must be tracked so a graceful shutdown can
	// wait for it instead of the process just exiting mid-flight (see
	// Shutdown).
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			bgCtx, cancel := context.WithTimeout(context.Background(), recordingProcessingTimeout)
			defer cancel()
			if err := s.processRecording(bgCtx, rec); err != nil {
				s.log.Error("process recording failed", "call_id", rec.CallID, "event_id", rec.EventID, "err", err)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done. ctx is expected to be independent of any inbound HTTP
// request's lifetime (see Ingest).
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}

// Shutdown blocks until all in-flight background work (currently, recording
// processing) has finished, or ctx is done, whichever comes first. Callers
// should invoke this during graceful shutdown, after the HTTP server has
// stopped accepting new requests, so in-flight recordings from requests that
// already returned 200 get a chance to finish instead of being silently
// dropped when the process exits.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
