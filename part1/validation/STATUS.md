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

## Final audit and runner correction

The final audit found and fixed an evidence-preservation issue: resuming an
experiment could overwrite earlier snapshot files. The runner now continues
capture numbering, reserves interrupted export numbers, retains complete earlier
observations in the report, and records the invocation start time on new captures.

Passed in this audit:
- All five unit/regression tests, including preservation of existing capture bytes,
  interrupted exports, mismatched run IDs, report numbering and session semantics.
- Two actual controlled runner invocations with the same run/topics/checkpoint:
  `resume-audit-20261110a/PASS.json` and its captured plots/screenshots. The original
  PNG/CSV/JSON/Kibana image bytes stayed unchanged, the second capture received a
  new number, entity counts stayed Microsoft 1/London 1, and the producer audit
  retained one unique article. The report includes both sessions.
- Stale Elasticsearch versions cannot roll back the latest count: version 1/count 3,
  then version 2/count 5, then a stale version 1 and equal-version retry left count 5,
  version 2 and two historical documents. See sink-version-audit-20261110a.json.
- Every live CSV count matches its JSON snapshot; all three batches contain exactly
  77 entity/label rows and 111 occurrences, and all six live PNGs decode correctly.
- The original committed evidence files matched their original SHA-256 hashes.
- The dependency lock exactly matches the installed environment; pip check, Python
  syntax checks, shell syntax, and Compose configuration validation passed.

setup.sh now installs the tested transitive lock when available. Existing BBC
captures have not been replaced or recollected. Docker image extraction remains
blocked by the storage issue described above; Compose runtime startup is not claimed
as tested. A 15/30/45/60-minute run was not performed; the actual live targets were
2/4/6 minutes, as documented. Add the instructor's cover sheet and student details
before academic submission.
