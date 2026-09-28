# dcre-crw

> Part of the DCRE fleet. For the fleet map, the rulings and the diagrams that specify every stage, start at the [DCRE design register](https://github.com/sean-huni/dcre-design-register); the complete list of live repositories is its [Repositories](https://github.com/sean-huni/dcre-design-register#repositories) table.

Collections Request Writer: the clock-windowed DCRE stage that emits pain.008 collection-order batch files (per-client `fint-req/out`) for validated work due on the run date.

## What it does

| | |
|---|---|
| Stage | `CRW` |
| Family / leg | Collections (DC), REQ |
| Trigger | clock-launched: AGT `CrwScheduler` launches one Job per window (`AGT_CRW_INTERVAL_SECONDS`, default 60) with identifying params `run.date=<yyyy-MM-dd>` and `window=<run.date>-w<n>` (R-16/R-37) |
| Upstream | `CDE` by data, not by DAG edge: CRW reads `cde_schedule`. CRW is not a DAG successor |
| Downstream | Fintegrate (pain.008 files in the per-client `fint-req/out`); the replies return through `CIX`, `CSX`, `CPX`. AGT keeps each DC arrival `DAG_RUNNING` until `crw_emission_owed` says nothing is owed |
| Diagram sheet | `dcre-collections-req` |

Position per AGT `RouteDags.DC` (`Emission.REQUIRED`) and `CrwScheduler` on origin/dev (checked 2026-09-28). CRW is the Process-Date Executor (R-37) for COLLECTIONS only: payments emit through PRW, a real DAG stage in the payments family, so there is no payments window job. Each window selects PASS-validated transactions whose `cde_schedule.process_date` equals the run date, freezes each due parent's membership into an immutable batch plan (at most `max-split-size` transactions per outbound file, SCRUM-55), and publishes the plan's synthetic pain.008 XML files in ordinal order into that client's `fint-req/out` exchange directory for Fintegrate; futured (scheduled-but-not-due) work stays warehoused, visible via R-38 exclusion WARNs.

## Architecture and principles

- SOLID, 3-tier, layer-first packages: the Batch tasklet (`EmissionTasklet`) is a thin per-lane entry adapter that calls one business-tier method; all logic lives in `service` (`LaneEmissionService` per lane, `EmissionService` per parent, `SplitPlanner` for the frozen plan); persistence only via `data/repo` (`CrwEmissionGroupRepo`, `CrwEmissionRepo`, `CrwEmissionMemberRepo`) with entities in `data/model` extending the platform `BaseEntity`. Single responsibility per class: `Pain008Writer` builds XML, `CrdbRetry` bounds retries, `ClientLanePartitioner` buckets lanes, `DueSql` owns the due-path SQL, platform-batch's `OutcomeSeamListener` owns the outcome seam.
- 12FactorApp Alignment - https://12factor.net/ : config strictly from the environment with committed working dev defaults (a clean clone runs with no `.env`), a stateless one-shot process, CockroachDB and the exchange filesystem as attached backing services.
- Idempotent restart semantics at batch grain, snapshot-first (R-24, SCRUM-55): per (arrival, run date) a plan group is claimed via `INSERT ... ON CONFLICT (arrival_id, run_date) DO NOTHING` (first writer wins) and its batches are claimed on the FULL identity `ON CONFLICT (arrival_id, run_date, batch_ordinal) DO NOTHING`. Members freeze set-based into `crw_emission_member` and each batch's `tx_count`/`control_sum` freeze with them; every file is built strictly from that immutable snapshot, never the live selection, and reconciles against its OWN frozen `tx_count` first. States walk `PLANNED -> MATERIALIZED -> VISIBLE`. A restart rebuilds the identical member set even after the schedule drifts; a config change never repartitions an existing plan (`applied_max` is frozen in the group).
- Outbound client (A-43, ruled 2026-08-08): `<client>` above is `tx_header.client_token`, the R-31 filename token, falling back to the copybook `initg_pty` when an arrival carried no filename. It is resolved ONCE, in `DueSql.CLIENT_EXPR`, so `crw_emission_group.client`, the file name and the per-client output directory cannot come from different sources. No emitted pain.008 element changes: the message body carries no initiating party.
- Outbound identity (SCRUM-55): an unsplit parent keeps exactly one batch with the bare source MsgId (`<MsgId>` unchanged, file `<client>_<msg_id>_PAIN008.xml`); a split parent's children are suffixed `_1.._N` (`<MsgId>` = `<msg_id>_N`, file `<client>_<msg_id>_<N>_PAIN008.xml`). A re-emission on a later run date (warehoused rows maturing) continues the parent's artifact sequence and is always suffixed, so outbound MsgIds never repeat (unique index on `outbound_msg_id`).
- Durable-effect ordering: the arrival transaction commits the WHOLE plan (group, batches, members, frozen totals, `MATERIALIZED`) BEFORE any file is written; publication then walks the batches strictly in ordinal order (`_2` never VISIBLE before `_1`), per batch `StagedWrite` (tmp + atomic move, restart no-op, R-05) then `markVisible` in its own small transaction. A kill between plan commit and publication resumes by publishing exactly the unpublished ordinals of the same frozen plan. A VISIBLE batch was already handed to Fintegrate: a later window never re-emits it (single file-level WARN `reason=ALREADY_VISIBLE` per batch, SCRUM-30/SCRUM-55).
- CRDB serialization aborts (SQLSTATE 40001) are retried, never skipped: the per-parent plan transaction and every per-batch publication transaction run `REQUIRES_NEW` wrapped in `CrdbRetry` (5 attempts, exponential backoff), and the production `emitWorkerStep` carries the shared platform `CrdbRetryExceptionHandler` at the step boundary. One failed parent never rolls back sibling emissions; the window still fails at the end so the next window resumes exactly the unplanned arrivals and unpublished batches (SCRUM-42 load fix, per-arrival reads instead of the whole-backlog join that blew CRDB's sql memory budget).
- Fail loud on a missing peer table (A-76, A-78): the due-path statements name `cde_schedule`, `validation_log`, `tx_entry` and `tx_header` with no presence guard, so a missing one fails the window rather than reporting clean, empty windows forever (`MissingScheduleTableIT`).

### Job structure

`crwJob` = partitioned step `emitStep` (SCRUM-55 Feature 2): `ClientLanePartitioner` buckets the run date's due clients into at most `min(pod CPUs, DCRE_CRW_MAX_PARTITIONS)` lanes (keys `lane-N`; the client list rides in each lane's ExecutionContext) executed concurrently on virtual threads. Within a lane clients are serial and a client's parents follow insertion-order FIFO eligibility (`tx_header.created_at`); each `emitWorkerStep` lane writes its emitted-FILE count into its own execution context as `emitted`. An `@Order(-10)` ApplicationRunner runs `StaleExecutionSweeper.abandonStale(ds, "CRW_BATCH_", 60)` before launch, abandoning STARTED executions older than 60 s so a killed pod cannot strand the relaunch (A-39a). `afterJob` on COMPLETED writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>` (staged, atomic); technical death writes nothing: the R-34 exit code (`ExitCodeMain`) and the K8s condition are the witnesses (R-33).

### Data

| Datasource | Database (dev default) | Env vars | Access |
|---|---|---|---|
| primary | `dcre_col` | `DCRE_DB_URL`, `DCRE_DB_USER`, `DCRE_DB_PASSWORD` | read/write |
| heartbeat (platform-batch) | `agt_ops` | `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER`, `DCRE_AGTOPS_DB_PASSWORD` | `HeartbeatWriter` liveness stamp |

Reads (grants-based): `cde_schedule` (CDE), `validation_log` (CTV), `tx_entry` + `tx_header` (CRR). Writes (CRW single-writer): `crw_emission_group` (UNIQUE `(arrival_id, run_date)` and `(client, source_msg_id, run_date)`), `crw_emission` (UNIQUE `(arrival_id, run_date, batch_ordinal)`, unique `outbound_msg_id`), `crw_emission_member` (UNIQUE `(emission_id, sequence)`), plus the outbound batch files: `<client>_<msg_id>_PAIN008.xml` (unsplit) or `<client>_<msg_id>_<N>_PAIN008.xml` (split children and later-run-date re-emissions). Publishes the view `crw_emission_owed` (`2026/08/003-emission-owed-view.xml`): per arrival, whether an emission is still owed. AGT reads it through its read-only collections datasource to gate DAG completion. The changeset carries `onFail="CONTINUE"` preconditions on `tx_header`, `validation_log`, `cde_schedule` and `crw_emission_group`, so on an empty database it is skipped without a history row and retried on the next start (A-79).

Liquibase owns all DDL (`2026/08/001-batch-metadata.xml`, `002-crw-emission.xml`, `003-emission-owed-view.xml`) with per-service history tables `crw_databasechangelog` / `crw_databasechangeloglock`; Spring Batch metadata sits under the `CRW_BATCH_` prefix (`dcre.batch.table-prefix`).

`Pain008Writer` is the SYNTHETIC-CONTRACT (R-35, A-9) pain.008-shaped skeleton: GrpHdr `MsgId` = the batch's outbound MsgId, `NbOfTxs`/`CtrlSum` = the batch's frozen totals, `PmtTpInf/LclInstrm/Cd = TT2` (R-02/R-18), one `DrctDbtTxInf` per member with the canonical EndToEndId (R-15) and `InstdAmt Ccy="ZAR"`. Real bindings become JAXB from the Fintegrate XSD profile when recovered.

## Prerequisites

- Java 25 (`.sdkmanrc`: `java=25-tem`; `build.gradle` sets source/target compatibility 25)
- Gradle 9.5.1 via the wrapper
- Docker (Testcontainers test suite and image build)
- Platform libs in Maven Local: `za.co.fnb.dcre:platform-persistence:0.1.0` and `za.co.fnb.dcre:platform-batch:0.1.0` (`platform-batch` brings `platform-files` and `platform-model` transitively; all resolve from `mavenLocal` only)
- A reachable CockroachDB for a real run (the dcre-infra kind cluster, or any CRDB at `DCRE_DB_URL`)

## Quickstart

```bash
# one-time: publish the platform libs (order matters for the batch chain)
# (paths assume the fleet checkout: collections/crw beside platform/*)
(cd ../../platform/platform-model && ./gradlew publishToMavenLocal)
(cd ../../platform/platform-files && ./gradlew publishToMavenLocal)
(cd ../../platform/platform-batch && ./gradlew publishToMavenLocal)
(cd ../../platform/platform-persistence && ./gradlew publishToMavenLocal)

./gradlew build        # compile + full test suite (Docker required)
```

Local one-shot run against the kind cluster's CRDB (dcre-infra `scripts/crdb-forward.sh` forwards host 26258 to cluster 26257):

```bash
DCRE_DB_URL="jdbc:postgresql://localhost:26258/dcre_col?sslmode=disable" \
  java -jar build/libs/crw-2.0.jar run.date=2026-07-15 window=w1
```

The JVM exit code carries the Batch verdict (R-34). A clean clone runs with NO `.env`: `application.yml` commits working dev defaults.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_col?sslmode=disable` | shared CockroachDB JDBC URL |
| `DCRE_DB_USER` | `root` | DB user |
| `DCRE_DB_PASSWORD` | (empty) | DB password |
| `DCRE_AGTOPS_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | heartbeat datasource |
| `DCRE_AGTOPS_DB_USER` / `DCRE_AGTOPS_DB_PASSWORD` | `root` / (empty) | heartbeat credentials |
| `DCRE_EXCHANGE_ROOT` | `../../../../../../infra/dcre-infra/exchange` | exchange tree root: per-client `fint-req/out` output + `outcomes/` seam |
| `DCRE_CRW_MAX_SPLIT_SIZE` | `5000` | max transactions per outbound pain.008 batch (`dcre.crw.split.max-size`) |
| `DCRE_CRW_MAX_PARTITIONS` | `5` | parallel client-lane cap; `PartitionSizer` clamps it to the pod's CPU count |
| `DCRE_AMOUNT_SCALE` | `2` | shared fleet key; not read by CRW code |
| `DCRE_V1_ENABLED` | `false` | shared fleet key; not read by CRW code |
| `JOB_NAME` | `local-crw-<executionId>` | outcome seam file name; set by AGT on minted Jobs |

This is the documented set, not a closed total: Spring relaxed binding lets any Spring or `dcre.*` property be overridden by its derived environment variable name.

Split sizing (SCRUM-55): the default `max-size` is 5000; a per-client override is a yml map entry `dcre.crw.split.overrides.[FNBCC01]: 10000` (unknown clients inherit the default); a JobLauncher program argument `--dcre.crw.split.max-size=N` wins over both by standard Spring property precedence (launch param > env > yml). The applied max freezes into the plan (`crw_emission_group.applied_max`), so later config changes never repartition an already-claimed run date.

`spring.config.import: classpath:dcre-exchange-layout.yml` (shipped in `platform-batch`, SCRUM-42) maps the per-client exchange leaves (FNBCC01, FNBCC02, FNBRF01); an unconfigured client fails closed at `ExchangeLayout.resolve`.

## Testing

```bash
./gradlew test   # Docker required
```

- `CrwJobTest`: end-to-end job contract on Testcontainers CockroachDB `cockroachdb/cockroach:v26.2.3` (the image every container-backed test pins).
- `CucumberSuiteTest` (BDD, `features/crw-process-date-executor.feature`): due-only emission with TT2, futured warehousing with per-tx R-38 WARNs, snapshot restart rebuilding the identical member set, ALREADY_VISIBLE cross-window suppression, and idempotent same-window rerun.
- `SplitPlannerIT`: snapshot-consistent split planning (12001 tx at max 5000 -> ordinals 1..3), bare-MsgId unsplit identity, cross-run-date artifact-sequence continuation, config-change replay never repartitioning.
- `EmissionSplitIT`: ordinal publication, per-batch restart keys, the plan-commit-before-publication seam kill/resume, day-two maturation emitting a new suffixed artifact, ALREADY_VISIBLE per batch.
- `SplitSchemaIT`: group and ordinal-batch schema round trip.
- `EmissionOwedViewIT` / `EmissionOwedViewBootstrapIT`: the shipped `crw_emission_owed` body (read from the changelog) over planned, visible, futured and zero-PASS arrivals; on a database without its peer tables the changeset is skipped with no history row and appears on the first migration after they exist.
- `MissingScheduleTableIT`: an absent `cde_schedule` (or any peer table) fails the window loudly, naming itself.
- `UnscheduledArrivalIT`: PASS rows with no schedule are neither due nor plannable.
- `ClientAuthorityIT` / `DueSqlAssemblyTest`: the outbound client is the filename token with the copybook header as fallback, used identically by lane universe and lane selection; due-SQL assembly checks.
- `CrwLaneDeterminismIT`: partitioned lanes (max-partitions 5) persist exactly what the sequential run (1) produces; insertion-order FIFO eligibility within a client.
- `ClientLanePartitionerTest` / `CrwSplitPropertiesTest`: bounded `lane-N` keys with the client list in the ExecutionContext; split config binding (default, per-client override, launch-param precedence).
- `EmissionServiceFuturedWarnTest`: R-38 WARN shape at 300k scale (per-tx detail up to 100 per group, one summary WARN above).
- `EmissionServiceRetryTest` / `CrwJobConfigRetryTest`: bounded CRDB 40001 retry at the service and step boundary.

## Local cluster deployment

```bash
VERSION=<fleet release tag>
./gradlew bootJar
docker build -t dcre-crw:$VERSION .
kind load docker-image --name dcre-dev dcre-crw:$VERSION
```

The Dockerfile (`eclipse-temurin:25-jre-alpine`) packages `build/libs/crw-2.0.jar` (the Gradle project version; the fleet release version is carried by the image and git tag, digits-only SemVer, no `v` prefix). AGT reads the image from `AGT_CRW_IMAGE` (empty by default, which also stops `CrwScheduler` launching); dcre-infra `scripts/switch-version.sh <version>` sets it to `dcre-crw:<version>` (checked 2026-09-28). Each window AGT creates a Job in the collections flow namespace (AGT `AGT_NAMESPACE_COL`, default `dcre-col`) with program args `run.date=<yyyy-MM-dd>` and `window=<run.date>-w<n>`, and env `JOB_NAME`, `DCRE_DB_URL` (AGT `service-db-url`, `dcre_col`), `DCRE_EXCHANGE_ROOT=/exchange`, `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER` (AGT `JobLauncher` on origin/dev, checked 2026-09-28).

## Related repositories

The complete, current list of live DCRE repositories (stage services, orchestrator, platform libraries, infra and tooling) lives in one place: the [DCRE design register README](https://github.com/sean-huni/dcre-design-register#repositories). Deprecated and archived repositories are deliberately absent from it. This README does not copy that list, so it cannot drift.

- Design register: https://github.com/sean-huni/dcre-design-register (start at `docs/specs/DESIGN-REGISTER.md`; the diagrams in `docs/diagrams/` are the specification)
