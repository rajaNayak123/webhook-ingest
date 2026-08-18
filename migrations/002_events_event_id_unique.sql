-- The service relied on an application-level "SELECT ... EventExists" check
-- before inserting, with no DB constraint backing it up. Two concurrent (or
-- merely close-together) deliveries of the same event_id could both pass the
-- exists-check before either had inserted, and both would then insert,
-- double-count call stats, and upsert the call row twice. The non-unique
-- index below never prevented that.
--
-- A unique constraint makes "insert this event_id" atomic and race-proof:
-- Postgres itself guarantees only one delivery of a given event_id ever
-- succeeds, no matter how many arrive concurrently.
DROP INDEX IF EXISTS idx_events_event_id;

ALTER TABLE events
    ADD CONSTRAINT events_event_id_key UNIQUE (event_id);
