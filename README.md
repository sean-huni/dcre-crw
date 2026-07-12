# dcre-crw

Collection Request Writer = Process-Date Executor (R-37): clock/PC-window job emitting pain.008 ONLY for transactions where run date == `cde_schedule.process_date`; futured work warehouses. Snapshot-first (R-24): membership claimed immutably before file build; restart reuses it. 3-tier; SYNTHETIC pain.008 skeleton (A-9); TT2 via LclInstrm/Cd (R-02/R-18).

## Pipeline position

Terminal Collections-DAG stage downstream of CDE (`CDE -> CRW`), causally independent of the parallel CIR branch. Unlike CTV/CDE it is not launched per arrival: AGT launches it on the clock/PC window with job identity (run date, window) per R-16/R-37. Boundary service (R-30): the only file it touches is the outbound Fintegrate XML; everything else transitions via the DB.

## Job structure and key rules

`crwJob` = single tasklet step `emitStep`: `EmissionTasklet` (thin entry adapter) -> `EmissionService` (business tier) -> `data/repo` + `Pain008Writer`. Identifying JobParameters: `run.date` (ISO date) and `window`. The count of emitted files lands in the execution context as `emitted`.

Per run date, `EmissionService.emitDue`:

1. R-38 exclusion visibility: one WARN per futured (scheduled-but-not-due) transaction, shape `excluded stage=CRW arrival=<id> seq=<n> e2e=<e2e> reason=FUTURED_<process_date>`.
2. Selects due rows (PASS verdict + `process_date = run date`, joined across `cde_schedule`/`validation_log`/`tx_entry`/`tx_header`), grouped per arrival.
3. Per arrival, snapshot-first (R-24): claims `crw_emission` via `INSERT ... ON CONFLICT (arrival_id, run_date) DO NOTHING` (first writer wins), then walks the state machine `PLANNED -> MATERIALIZED -> VISIBLE`. Members are frozen into `crw_emission_member` while PLANNED; the file is built strictly from that immutable snapshot, never the live selection, so a restart rebuilds the identical member set even when the schedule has moved on.
4. Cross-window duplicate suppression (SCRUM-30): a VISIBLE emission was already handed to Fintegrate by an earlier window of the same run date; a later window MUST NOT re-emit (duplicate collection order). Skip is a single file-level WARN `... seq=-1 e2e=- reason=ALREADY_VISIBLE`. Restart rebuilds still happen while the state is pre-VISIBLE.
5. Writes `<exchange-root>/fint-req/<initg_pty>_<msg_id>_PAIN008.xml` via `StagedWrite` (atomic; restart no-op, R-05), then transitions to VISIBLE.

`Pain008Writer` is the [SYNTHETIC-CONTRACT R-35, A-9] pain.008-shaped skeleton: GrpHdr MsgId/NbOfTxs/CtrlSum (sum of member amounts), `PmtTpInf/LclInstrm/Cd = TT2` (R-02/R-18), one DrctDbtTxInf per member with the canonical EndToEndId byte-preserved (R-15) and `InstdAmt Ccy="ZAR"`. Real bindings become JAXB from the Fintegrate XSD profile when recovered.

## Outcome seam

`afterJob` on COMPLETED writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>` (staged, atomic). Technical death writes nothing: the R-34 exit code (`ExitCodeMain`) and the K8s condition are the witnesses; AGT treats absence as never-success (R-33).

## Data

Reads (grants-based, R-04/R-06): `cde_schedule` (CDE), `validation_log` (CTV), `tx_entry` + `tx_header` (CRR). Writes: `crw_emission` (UNIQUE(arrival_id, run_date)) and `crw_emission_member` (UNIQUE(emission_id, sequence)), both CRW single-writer (R-04), plus the outbound pain.008 file.

Liquibase: per-service history tables `crw_databasechangelog` / `crw_databasechangeloglock` (shared DB). Changesets: 001 `crw_emission` + `crw_emission_member` (immutable emission snapshot, R-24), 002 CRW_BATCH_ metadata DDL.

## Batch metadata

Spring Batch tables under the `CRW_BATCH_` prefix, `initialize-schema: never` (Liquibase owns the DDL). A-39a self-abandonment: an `@Order(-10)` ApplicationRunner runs `StaleExecutionSweeper.abandonStale(ds, "CRW_BATCH_", 60)` before the job launches, abandoning STARTED executions older than 60 s so a killed pod cannot strand the relaunch.

## Local module dependencies

| Module | Version | Scope | Used for |
|---|---|---|---|
| `dcre-platform-persistence` | 0.1.0 | `implementation` | `BaseEntity` (version/created_at/updated_at on `CrwEmissionEntity`/`CrwEmissionMemberEntity`), `JdbcConfig` (Spring Data JDBC base config, imported by `CrwApplication`) |
| `dcre-platform-batch` | 0.1.0 | `implementation` | `ExitCodeMain` (R-34 exit-code wiring), `OutcomeFileWriter` (outcome seam), `StaleExecutionSweeper` (A-39a self-abandonment) |

`StagedWrite` (the atomic pain.008 write) comes from `dcre-platform-files`, not declared directly: it arrives transitively via `dcre-platform-batch`'s `api` chain (batch brings files brings model). All artifacts resolve from Maven Local only (no remote repository): run `./gradlew publishToMavenLocal` in each dependency repo first, publish chain `dcre-platform-model` -> `dcre-platform-files` -> `dcre-platform-batch`; `dcre-platform-persistence` is standalone. Details in each module repo's README under "Publishing".

## Configuration

12FactorApp Alignment (https://12factor.net/): committed working dev defaults, env overrides; a clean clone runs with no `.env`.

| Env var | Default | Used for |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_collections?sslmode=disable` | shared CockroachDB |
| `DCRE_DB_USER` / `DCRE_DB_PASSWORD` | `root` / empty | DB credentials |
| `DCRE_EXCHANGE_ROOT` | `../../infra/dcre-infra/exchange` | `fint-req/` output + outcome seam |
| `JOB_NAME` | `local-<executionId>` | outcome seam file name (set by AGT) |

`DCRE_AMOUNT_SCALE`, `DCRE_V1_ENABLED` and `DCRE_FLOW_DC` sit in the shared config block but are not consumed by CRW code.

## Build & test

Spring Boot 4.1.0, Java 25 toolchain; platform libs resolve from mavenLocal (see Local module dependencies). `./gradlew test` (Docker required): `CrwJobTest` on Testcontainers CockroachDB v26.2.3 covers the whole contract in one scenario: only due-today rows emitted (futured rows warehoused with one R-38 WARN each), TT2 present, snapshot immutability (schedule drifts + pre-VISIBLE crash state + deleted file -> rerun rebuilds the identical members), and cross-window suppression (later window of the same run date re-emits nothing, single ALREADY_VISIBLE WARN).

## Run

`./gradlew build && docker build -t dcre-crw:0.1.0 .` (eclipse-temurin:25-jre-alpine). In the cluster AGT launches it as a Job on the PC window with `JOB_NAME` and the identifying `run.date=<yyyy-MM-dd> window=<id>` job parameters; locally: `java -jar build/libs/dcre-crw-0.1.0.jar run.date=2026-07-12 window=w1` against the dcre-infra compose stack. The JVM exit code carries the Batch outcome (R-34).

## Observability

No metrics wired yet. Operational signals: structured R-38 exclusion WARNs (`excluded stage=CRW ... reason=FUTURED_*` / `reason=ALREADY_VISIBLE`), the outcome seam file, and the R-34 exit code observed by AGT.
