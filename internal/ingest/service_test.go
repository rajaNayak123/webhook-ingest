package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

// TestConcurrentDuplicateDeliveriesDoNotDoubleCount reproduces the ops
// report directly: the same event_id, redelivered while the first delivery
// is still in flight (exactly what an at-least-once provider does on a slow
// or lost ack), must never double-count. This is the scenario the old
// check-then-act (EventExists, then InsertEvent) could not protect against,
// because both requests could pass the check before either had inserted.
func TestConcurrentDuplicateDeliveriesDoNotDoubleCount(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const deliveries = 20
	var wg sync.WaitGroup
	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := post(t, srv.URL+"/webhooks/calls", body)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("delivery got %d, want 200", resp.StatusCode)
			}
		}()
	}
	wg.Wait()

	var events int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&events); err != nil {
		t.Fatalf("scan events: %v", err)
	}
	if events != 1 {
		t.Fatalf("stored %d event rows for %d concurrent deliveries of the same event_id, want 1", events, deliveries)
	}

	var calls int
	row = st.Pool().QueryRow(ctx, `SELECT count(*) FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&calls); err != nil {
		t.Fatalf("scan calls: %v", err)
	}
	if calls != 1 {
		t.Fatalf("stored %d call rows, want 1", calls)
	}

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 {
		t.Fatalf("account call_count = %d after %d concurrent duplicate deliveries, want 1 (this is the drift ops reported)", got.CallCount, deliveries)
	}
	if got.TotalDurationSec != 143 {
		t.Fatalf("account total_duration_sec = %d, want 143 (single call's duration, not %d x)", got.TotalDurationSec, deliveries)
	}
}

// TestRecordingGetsMarkedProcessedAfterResponseReturns reproduces "calls are
// landing but their recordings never get marked processed, and there's
// nothing in the logs about it". The old code ran processRecording on the
// inbound request's context. net/http cancels that context the moment the
// handler returns -- well before the goroutine's 50ms sleep elapses -- so
// MarkRecordingProcessed always failed with a cancelled-context error that
// was silently discarded by a bare `// TODO: handle`.
func TestRecordingGetsMarkedProcessedAfterResponseReturns(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
	// The handler has already returned and the response body has been read,
	// so (under the old code) the request's context is cancelled by now.

	deadline := time.Now().Add(2 * time.Second)
	for {
		var processed bool
		row := st.Pool().QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
		if err := row.Scan(&processed); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if processed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("recording was never marked processed after the response returned")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestShutdownWaitsForInFlightRecordingProcessing reproduces "every time we
// deploy, whatever was in flight seems to just disappear": nothing tracked
// or waited for the background recording-processing goroutine, so a
// shutdown that raced it would simply kill it. Shutdown must block until
// that work has actually finished.
func TestShutdownWaitsForInFlightRecordingProcessing(t *testing.T) {
	svc, st := testutil.NewService(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  5,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Now(),
	}
	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := svc.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown did not finish draining in-flight work: %v", err)
	}

	var processed bool
	row := st.Pool().QueryRow(ctx,
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording to be processed by the time Shutdown returned")
	}
}
