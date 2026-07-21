package za.co.fnb.dcre.crw;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import za.co.fnb.dcre.crw.service.EmissionService;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class CrwJobTest {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    @Autowired
    Job crwJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JdbcTemplate jdbc;

    void seed(UUID arrival, String client, String msgId, int total, String dueDate, String futureDate) {
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_header (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID UNIQUE, msg_id VARCHAR(35), initg_pty VARCHAR(35),"
                + " created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_entry (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, e2e VARCHAR(35), amount DECIMAL(18,2), UNIQUE (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS validation_log (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, outcome VARCHAR(32), UNIQUE (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS cde_schedule (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, process_date DATE, UNIQUE (arrival_id, sequence))");
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty) VALUES (?,?,?)",
                arrival, msgId, client);
        for (int i = 1; i <= total; i++) {
            jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, e2e, amount) VALUES (?,?,?,?)"
                    + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, i, "E2E" + msgId + i, i * 10.0);
            jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,'PASS')"
                    + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, i);
            // odd sequences due today, even sequences futured (R-37 warehousing)
            jdbc.update("INSERT INTO cde_schedule (arrival_id, sequence, process_date) VALUES (?,?,?::date)"
                    + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, i,
                    i % 2 == 1 ? dueDate : futureDate);
        }
    }

    @Test
    void emitsOnlyRowsDueTodayAndSnapshotIsImmutable() throws Exception {
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFCRW" + arrival.toString().substring(0, 6);
        seed(arrival, "FNBRF01", msgId, 6, "2026-07-12", "2026-07-20");

        Logger emissionLogger = (Logger) LoggerFactory.getLogger(EmissionService.class);
        ListAppender<ILoggingEvent> warns = new ListAppender<>();
        warns.start();
        emissionLogger.addAppender(warns);

        JobExecution run = jobOperator.start(crwJob, new JobParametersBuilder()
                .addString("run.date", "2026-07-12", true)
                .addString("window", "w1", true).toJobParameters());
        assertEquals(BatchStatus.COMPLETED, run.getStatus());

        // R-38 exclusion visibility: one WARN per futured (scheduled-but-not-due) row.
        List<String> futuredWarns = warns.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.contains("excluded stage=CRW") && m.contains("reason=FUTURED_2026-07-20"))
                .toList();
        assertEquals(3, futuredWarns.size(),
                "one WARN per futured row (even sequences, R-38 exclusion visibility)");
        assertEquals("excluded stage=CRW arrival=" + arrival + " seq=2 e2e=E2E" + msgId
                + "2 reason=FUTURED_2026-07-20", futuredWarns.get(0), "uniform R-38 WARN shape");

        // SCRUM-42: pain.008 now lands under the per-client fint-req/out leaf, not the flat channel dir.
        Path file = Path.of("build/test-exchange/fnbrf01/fint-req/out", "FNBRF01_" + msgId + "_PAIN008.xml");
        List<String> xml = Files.readAllLines(file);
        assertEquals(1, xml.stream().filter(l -> l.contains("<NbOfTxs>3</NbOfTxs>")).count(),
                "only the 3 odd (due-today) rows emitted; futured rows warehoused (R-37)");
        assertTrue(xml.stream().anyMatch(l -> l.contains("<Cd>TT2</Cd>")), "R-02 TT2 assertion slot");

        // Snapshot immutability (R-24): another row becomes due, the write is rolled
        // back to a pre-VISIBLE crash state, file deleted, rerun -> SAME members.
        jdbc.update("UPDATE cde_schedule SET process_date='2026-07-12' WHERE arrival_id=? AND sequence=2", arrival);
        jdbc.update("UPDATE crw_emission SET state='MATERIALIZED' WHERE arrival_id=?", arrival);
        Files.delete(file);
        JobExecution rerun = jobOperator.start(crwJob, new JobParametersBuilder()
                .addString("run.date", "2026-07-12", true)
                .addString("window", "w2", true).toJobParameters());
        assertEquals(BatchStatus.COMPLETED, rerun.getStatus());
        List<String> xml2 = Files.readAllLines(file);
        assertTrue(xml2.stream().anyMatch(l -> l.contains("<NbOfTxs>3</NbOfTxs>")),
                "restart rebuilt from the immutable snapshot, not the drifted live selection");

        // Cross-window duplicate suppression: state is VISIBLE now; a later window
        // of the same run date must NOT re-emit the collection order.
        Files.delete(file);
        JobExecution later = jobOperator.start(crwJob, new JobParametersBuilder()
                .addString("run.date", "2026-07-12", true)
                .addString("window", "w3", true).toJobParameters());
        assertEquals(BatchStatus.COMPLETED, later.getStatus());
        assertTrue(Files.notExists(file), "VISIBLE emission re-sent by a later window");

        // R-38: the later-window skip is a single file-level WARN, seq=-1 e2e=-.
        assertTrue(warns.list.stream()
                        .filter(e -> e.getLevel() == Level.WARN)
                        .map(ILoggingEvent::getFormattedMessage)
                        .anyMatch(m -> m.equals("excluded stage=CRW arrival=" + arrival
                                + " seq=-1 e2e=- reason=ALREADY_VISIBLE batch=1")),
                "file-level ALREADY_VISIBLE WARN (R-38 exclusion visibility, SCRUM-55 batch grain)");
        emissionLogger.detachAppender(warns);
    }

    @Test
    void localSeamFallbackNameIsSelfDescribing() throws Exception {
        // SCRUM-58: without JOB_NAME env the outcome seam file must carry the
        // self-describing fleet-wide fallback local-crw-<executionId>.
        assumeTrue(System.getenv("JOB_NAME") == null, "seam fallback test requires no JOB_NAME in the environment");
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFCRW" + arrival.toString().substring(0, 6);
        // Isolated run date keeps this arrival out of the other tests' windows.
        seed(arrival, "FNBRF01", msgId, 1, "2026-05-05", "2026-05-12");

        JobExecution run = jobOperator.start(crwJob, new JobParametersBuilder()
                .addString("run.date", "2026-05-05", true)
                .addString("window", "ws", true).toJobParameters());
        assertEquals(BatchStatus.COMPLETED, run.getStatus());

        Path outcome = Path.of("build/test-exchange/outcomes", "local-crw-" + run.getId());
        assertTrue(Files.exists(outcome),
                "local seam fallback must be self-describing: local-crw-<executionId> (SCRUM-58)");
        assertEquals(List.of("BUSINESS_ACCEPTED"), Files.readAllLines(outcome),
                "verdict semantics preserved byte-exact across the OutcomeSeamListener swap");
    }

    @Test
    void failedArrivalDoesNotRollBackCommittedSiblingEmissions() throws Exception {
        UUID good = UUID.randomUUID();
        UUID bad = UUID.randomUUID();
        String goodMsg = "DCRERFCRW" + good.toString().substring(0, 6);
        String badMsg = "DCRERFCRW" + bad.toString().substring(0, 6);
        // Isolated run date keeps these arrivals out of the other tests' windows.
        // FNBXX99 has no configured exchange dirs: its arrival fails closed.
        seed(good, "FNBRF01", goodMsg, 1, "2026-03-02", "2026-03-09");
        seed(bad, "FNBXX99", badMsg, 1, "2026-03-02", "2026-03-09");

        JobExecution run = jobOperator.start(crwJob, new JobParametersBuilder()
                .addString("run.date", "2026-03-02", true)
                .addString("window", "wf", true).toJobParameters());

        assertEquals(BatchStatus.FAILED, run.getStatus(),
                "any failed arrival must keep the window outcome honest (job FAILED)");
        // SCRUM-42 load fix: the good arrival's emission is COMMITTED in its own
        // REQUIRES_NEW transaction despite the sibling failure (progress ratchets).
        assertEquals("VISIBLE", jdbc.queryForObject(
                        "SELECT state FROM crw_emission WHERE arrival_id = ?", String.class, good),
                "sibling failure must not roll back the good arrival's committed emission");
        Path file = Path.of("build/test-exchange/fnbrf01/fint-req/out", "FNBRF01_" + goodMsg + "_PAIN008.xml");
        assertTrue(Files.readAllLines(file).stream().anyMatch(l -> l.contains("<NbOfTxs>1</NbOfTxs>")),
                "good arrival's pain.008 written and handed over");
        // SCRUM-55 durable-effect ordering: the failed arrival's PLAN is committed
        // (MATERIALIZED) but nothing was published: no file, no VISIBLE state. The
        // next window resumes exactly the unpublished batches of the SAME plan.
        assertEquals("MATERIALIZED", jdbc.queryForObject(
                        "SELECT state FROM crw_emission WHERE arrival_id = ?", String.class, bad),
                "failed arrival's plan is durable yet unpublished; a later window resumes it");
    }

    @Test
    void unconfiguredClientFailsClosed() throws Exception {
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFCRW" + arrival.toString().substring(0, 6);
        // FNBXX99 has no configured exchange dirs. An isolated past run date (2026-01-01)
        // keeps this arrival out of the other test's findDue/findFutured windows.
        seed(arrival, "FNBXX99", msgId, 1, "2026-01-01", "2026-01-08");

        JobExecution run = jobOperator.start(crwJob, new JobParametersBuilder()
                .addString("run.date", "2026-01-01", true)
                .addString("window", "wx", true).toJobParameters());

        assertEquals(BatchStatus.FAILED, run.getStatus(),
                "an unconfigured client must fail closed via ExchangeLayout.resolve, never emit to a shared/wrong dir");
        assertTrue(Files.notExists(Path.of("build/test-exchange/fint-req", "FNBXX99_" + msgId + "_PAIN008.xml")),
                "no flat-dir fallback write for an unconfigured client");
        // SCRUM-42: nor may it land under the per-client fint-req/out leaf the new code actually writes to.
        Path perClientDir = Path.of("build/test-exchange/fnbxx99/fint-req/out");
        assertTrue(Files.notExists(perClientDir.resolve("FNBXX99_" + msgId + "_PAIN008.xml")),
                "no per-client write for an unconfigured client (SCRUM-42)");
        if (Files.exists(perClientDir)) {
            try (var staged = Files.list(perClientDir)) {
                assertTrue(staged.noneMatch(p -> p.getFileName().toString().contains(msgId)),
                        "no per-client PAIN008 staged for an unconfigured client");
            }
        }
    }
}
