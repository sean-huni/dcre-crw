# dcre-crw

Collections Request Writer: the clock-windowed DCRE stage that emits pain.008 collection-order files (per-client `fint-req/out`) for validated work due on the run date.

## What it does

CRW is the Process-Date Executor (R-37), the terminal Collections-DAG stage downstream of CDE. Unlike the per-arrival stages, AGT launches it as a short-lived Kubernetes Job on the clock/PC window with identifying job parameters `(run.date, window)` per R-16/R-37. It selects PASS-validated transactions whose `cde_schedule.process_date` equals the run date, freezes membership into an immutable snapshot, and writes one synthetic pain.008 XML per arrival into that client's `fint-req/out` exchange directory for Fintegrate; futured (scheduled-but-not-due) work stays warehoused, visible via R-38 exclusion WARNs.

## Architecture and principles

- SOLID, 3-tier, layer-first packages: the Batch tasklet (`EmissionTasklet`) is a thin entry adapter that calls one business-tier method; all logic lives in `service/EmissionService`; persistence only via `data/repo` (`CrwEmissionRepo`, `CrwEmissionMemberRepo`) with entities in `data/model` extending the platform `BaseEntity`. Single responsibility per class: `Pain008Writer` builds XML, `CrdbRetry` bounds retries, `SeamListener` owns the outcome seam.
- 12FactorApp Alignment - https://12factor.net/ : config strictly from the environment with committed working dev defaults (a clean clone runs with no `.env`), a stateless one-shot process, CockroachDB and the exchange filesystem as attached backing services.
- Idempotent restart semantics, snapshot-first (R-24): per (arrival, run date) the emission is claimed via `INSERT ... ON CONFLICT (arrival_id, run_date) DO NOTHING` (first writer wins), then walks `PLANNED -> MATERIALIZED -> VISIBLE`. Members freeze into `crw_emission_member` while PLANNED; the file is built strictly from that immutable snapshot, never the live selection, so a restart rebuilds the identical member set even after the schedule drifts. `StagedWrite` (tmp + atomic move) makes the file write a restart no-op (R-05). A VISIBLE emission was already handed to Fintegrate: a later window of the same run date never re-emits (single file-level WARN `reason=ALREADY_VISIBLE`, SCRUM-30).
- CRDB serialization aborts (SQLSTATE 40001) are retried, never skipped: each arrival commits in its own `REQUIRES_NEW` transaction wrapped in `CrdbRetry` (5 attempts, exponential backoff), and the production `emitStep` carries the shared platform `CrdbRetryExceptionHandler` at the step boundary. One failed arrival never rolls back sibling emissions; the window still fails at the end so the next window re-picks exactly the unclaimed arrivals (SCRUM-42 load fix, per-arrival reads instead of the whole-backlog join that blew CRDB's sql memory budget).

### Job structure

`crwJob` = single tasklet step `emitStep`; the emitted-file count lands in the execution context as `emitted`. An `@Order(-10)` ApplicationRunner runs `StaleExecutionSweeper.abandonStale(ds, "CRW_BATCH_", 60)` before launch, abandoning STARTED executions older than 60 s so a killed pod cannot strand the relaunch (A-39a). `afterJob` on COMPLETED writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>` (staged, atomic); technical death writes nothing: the R-34 exit code (`ExitCodeMain`) and the K8s condition are the witnesses (R-33).

### Data

Reads (grants-based): `cde_schedule` (CDE), `validation_log` (CTV), `tx_entry` + `tx_header` (CRR). Writes (CRW single-writer): `crw_emission` (UNIQUE `(arrival_id, run_date)`), `crw_emission_member` (UNIQUE `(emission_id, sequence)`), plus the outbound `<initg_pty>_<msg_id>_PAIN008.xml`. Liquibase owns all DDL with per-service history tables `crw_databasechangelog` / `crw_databasechangeloglock` on the shared DB; Spring Batch metadata sits under the `CRW_BATCH_` prefix with `initialize-schema: never`.

`Pain008Writer` is the SYNTHETIC-CONTRACT (R-35, A-9) pain.008-shaped skeleton: GrpHdr `MsgId`/`NbOfTxs`/`CtrlSum` (sum of member amounts), `PmtTpInf/LclInstrm/Cd = TT2` (R-02/R-18), one `DrctDbtTxInf` per member with the canonical EndToEndId (R-15) and `InstdAmt Ccy="ZAR"`. Real bindings become JAXB from the Fintegrate XSD profile when recovered.

## Prerequisites

- Java 25 (Gradle toolchain; wrapper 9.5.1 included)
- Docker (Testcontainers test suite and image build)
- Platform libs in Maven Local: `za.co.fnb.dcre:platform-persistence:0.1.0` and `za.co.fnb.dcre:platform-batch:0.1.0` (`platform-batch` brings `platform-files` and `platform-model` transitively; all resolve from `mavenLocal` only)
- A reachable CockroachDB for a real run (the dcre-infra kind cluster, or any CRDB at `DCRE_DB_URL`)

## Quickstart

```bash
# one-time: publish the platform libs (order matters for the batch chain)
(cd ../platform-model && ./gradlew publishToMavenLocal)
(cd ../platform-files && ./gradlew publishToMavenLocal)
(cd ../platform-batch && ./gradlew publishToMavenLocal)
(cd ../platform-persistence && ./gradlew publishToMavenLocal)

./gradlew build        # compile + full test suite (Docker required)
```

Local one-shot run against the kind cluster's CRDB (dcre-infra `scripts/crdb-forward.sh` forwards host 26258 to cluster 26257):

```bash
DCRE_DB_URL="jdbc:postgresql://localhost:26258/dcre_collections?sslmode=disable" \
  java -jar build/libs/crw-2.0.jar run.date=2026-07-15 window=w1
```

The JVM exit code carries the Batch verdict (R-34). A clean clone runs with NO `.env`: `application.yml` commits working dev defaults.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_collections?sslmode=disable` | shared CockroachDB JDBC URL |
| `DCRE_DB_USER` | `root` | DB user |
| `DCRE_DB_PASSWORD` | (empty) | DB password |
| `DCRE_EXCHANGE_ROOT` | `../../../../../infra/dcre-infra/exchange` | exchange tree root: per-client `fint-req/out` output + `outcomes/` seam |
| `JOB_NAME` | `local-<executionId>` | outcome seam file name; set by AGT on minted Jobs |

`spring.config.import: classpath:dcre-exchange-layout.yml` (shipped in `platform-batch`, SCRUM-42) maps the per-client exchange leaves (FNBCC01, FNBCC02, FNBRF01); an unconfigured client fails closed at `ExchangeLayout.resolve`. `DCRE_AMOUNT_SCALE`, `DCRE_V1_ENABLED` and `DCRE_FLOW_DC` sit in the shared `dcre` config block but are not consumed by CRW code.

## Testing

```bash
./gradlew test   # Docker required
```

- `CrwJobTest`: end-to-end job contract on Testcontainers CockroachDB `v26.2.3`.
- `CucumberSuiteTest` (BDD, `features/crw-process-date-executor.feature`): due-only emission with TT2, futured warehousing with per-tx R-38 WARNs, snapshot restart rebuilding the identical member set, ALREADY_VISIBLE cross-window suppression, and idempotent same-window rerun.
- `EmissionServiceFuturedWarnTest`: R-38 WARN shape at 300k scale (per-tx detail up to 100 per group, one summary WARN above).
- `EmissionServiceRetryTest` / `CrwJobConfigRetryTest`: bounded CRDB 40001 retry at the service and step boundary.

## Local cluster deployment

```bash
./gradlew bootJar
docker build -t dcre-crw:2.1.1 .
kind load docker-image --name dcre-dev dcre-crw:2.1.1
```

The Dockerfile (`eclipse-temurin:25-jre-alpine`) packages `build/libs/crw-2.0.jar` (the Gradle project version; the fleet release version is carried by the image and git tag, digits-only SemVer, no `v` prefix). In the cluster, AGT mints CRW as a short-lived Kubernetes Job on the PC window: the image comes from AGT's `AGT_CRW_IMAGE` env (set fleet-wide by dcre-infra `scripts/switch-version.sh`), with `JOB_NAME` and the identifying parameters `run.date=<yyyy-MM-dd> window=<id>`.

## Related repositories

- Orchestrator: [dcre-agt](https://github.com/sean-huni/dcre-agt)
- Stage services: [dcre-crr](https://github.com/sean-huni/dcre-crr), [dcre-ctv](https://github.com/sean-huni/dcre-ctv), [dcre-cde](https://github.com/sean-huni/dcre-cde), [dcre-cir](https://github.com/sean-huni/dcre-cir), [dcre-ixr](https://github.com/sean-huni/dcre-ixr), [dcre-sxr](https://github.com/sean-huni/dcre-sxr), [dcre-pxr](https://github.com/sean-huni/dcre-pxr), [dcre-prg](https://github.com/sean-huni/dcre-prg), [dcre-ais](https://github.com/sean-huni/dcre-ais), [dcre-hcs](https://github.com/sean-huni/dcre-hcs)
- Platform libs: [dcre-platform-model](https://github.com/sean-huni/dcre-platform-model), [dcre-platform-files](https://github.com/sean-huni/dcre-platform-files), [dcre-platform-batch](https://github.com/sean-huni/dcre-platform-batch), [dcre-platform-persistence](https://github.com/sean-huni/dcre-platform-persistence)
- Environment and tooling: [dcre-infra](https://github.com/sean-huni/dcre-infra), [dcre-fixture-toolkit](https://github.com/sean-huni/dcre-fixture-toolkit), [dcre-design-register](https://github.com/sean-huni/dcre-design-register), [dcre-rpt](https://github.com/sean-huni/dcre-rpt)
