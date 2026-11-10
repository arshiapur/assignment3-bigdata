# Validation in the resumed session

Newly executed checks passed:
- Dependency installation, `pip check`, real spaCy model extraction, and two unit tests.
- Official service distribution SHA-512 verification.
- Local RSS fixture -> real Kafka acknowledgement; cleaned text and audit logs;
  producer restart with SQLite deduplication (one article, second poll zero new).
- Kafka -> Spark -> Kafka -> Logstash -> Elasticsearch cumulative NER counts.
- Duplicate article ID within the initial input and in a later micro-batch.
- Spark restart with the identical checkpoint, followed by a replayed ID and new ID.
- Actual Kibana dashboard rendering (positive SVG bar geometry), with two screenshots.
- Replaying the final cumulative snapshot twice: latest counts unchanged and history
  document count unchanged at 15.

Evidence is in `smoke-20261110a/`: PASS.json, validation.log, sink-replay.json,
PNG/CSV/JSON observations, screenshots and Spark logs. The first-stage counts were
Obama 2, Paris 1, France 1, Microsoft 1, London 1; the second stage incremented only
Microsoft/London to 2. Restart incremented those two to 3 and left the other counts
unchanged. These are CONTROLLED TESTS, not BBC observations.

Docker Compose configuration parsed, but image extraction failed in this cloud's
rootless Docker storage with `failed to register layer ... no space left on device`.
The working run uses the same pinned service versions as official local binaries
via local_stack.py. Docker startup of this Compose file remains unverified.

The first controlled screenshots show a Vega sizing warning and a Kibana security
notification; the bars themselves rendered and were checked. The live-run capture
code removes the fixed dimensions and dismisses the notification. Services use a
loopback development configuration with authentication disabled.

Earlier historical validation claims apply to unavailable earlier code. The new
checks above apply to this repository. New live observations are under results/.
