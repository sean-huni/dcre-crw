package za.co.fnb.dcre.crw;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-55 Feature 2 determinism (CrrPartitionDeterminismTest shape): the
 * per-client partitioned emitStep must persist EXACTLY what the sequential
 * run produces. Two @Nested contexts run the same normalized fixture
 * (max-partitions 1 vs 5, split max 3) on their own run dates; the outer
 * @AfterAll proves BOTH ran and their normalized projections are identical.
 * Also proves insertion-order FIFO eligibility (Sean ruling 9): within one
 * client, group creation follows tx_header insertion order, not msg-id or
 * arrival-uuid order.
 */
class CrwLaneDeterminismIT {

    /** Real fan-out needs >= 2 CPUs; PartitionSizer clamps lanes to the pod's CPU count. */
    static final boolean MULTI_CPU = Runtime.getRuntime().availableProcessors() >= 2;

    /** Reuses the shared split-IT container (same bootstrap, one less CRDB container). */
    static void registerDb(final DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CrwTestcontainersBase.CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CrwTestcontainersBase.CRDB::getUsername);
        registry.add("spring.datasource.password", CrwTestcontainersBase.CRDB::getPassword);
    }

    /** Normalized projection keyed by max-partitions; both nested contexts contribute. */
    static final Map<Integer, List<String>> PROJECTIONS = new ConcurrentHashMap<>();

    @AfterAll
    static void bothContextsRanAndAgreed() {
        assertEquals(2, PROJECTIONS.size(),
                "both nested contexts must run: the cross-comparison needs 2 projections");
        assertEquals(12, PROJECTIONS.get(1).size(),
                "4 group rows + 8 ordinal batch rows per context (2 clients x [7tx split@3 + 2tx unsplit])");
        assertEquals(PROJECTIONS.get(1), PROJECTIONS.get(5),
                "partitioned client lanes deviate from the sequential run");
    }

    static void seedLaneFixture(final JdbcTemplate jdbc, final String prefix, final LocalDate runDate,
            final int contextTag) {
        ensureSpine(jdbc);
        // Insertion-order probe: within each client the FIRST-inserted parent carries the
        // LEXICALLY LATER msg id AND the HIGHER arrival uuid, so only true insertion-order
        // eligibility (tx_header.created_at) can put it first.
        seedParent(jdbc, laneUuid("ffffffff", contextTag), "FNBRF01", prefix + "RF2", 7, runDate,
                ts(runDate, 0));
        seedParent(jdbc, laneUuid("00000000", contextTag), "FNBRF01", prefix + "RF1", 2, runDate,
                ts(runDate, 5));
        seedParent(jdbc, laneUuid("eeeeeeee", contextTag), "FNBCC01", prefix + "CC2", 7, runDate,
                ts(runDate, 10));
        seedParent(jdbc, laneUuid("11111111", contextTag), "FNBCC01", prefix + "CC1", 2, runDate,
                ts(runDate, 15));
    }

    static UUID laneUuid(final String block, final int contextTag) {
        return UUID.fromString("%s-0000-4000-8000-%012d".formatted(block, contextTag));
    }

    static String ts(final LocalDate runDate, final int offsetSeconds) {
        return "%s 08:00:%02d+00".formatted(runDate, offsetSeconds);
    }

    static void ensureSpine(final JdbcTemplate jdbc) {
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_header (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID UNIQUE, msg_id VARCHAR(35), initg_pty VARCHAR(35),"
                + " flow VARCHAR(8) NOT NULL DEFAULT 'COL',"
                + " created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_entry (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, e2e VARCHAR(35), amount DECIMAL(18,2), UNIQUE (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS validation_log (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, outcome VARCHAR(32), UNIQUE (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS cde_schedule (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, process_date DATE, UNIQUE (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS ais_verdict (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, action VARCHAR(16), UNIQUE (arrival_id, sequence))");
    }

    static void seedParent(final JdbcTemplate jdbc, final UUID arrival, final String client,
            final String msgId, final int total, final LocalDate runDate, final String createdAt) {
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, created_at)"
                + " VALUES (?,?,?,?::TIMESTAMPTZ)", arrival, msgId, client, createdAt);
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, e2e, amount)"
                + " SELECT ?, i, 'E2E" + msgId + "' || i::STRING, 10.00 FROM generate_series(1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, total);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome)"
                + " SELECT ?, i, 'PASS' FROM generate_series(1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, total);
        jdbc.update("INSERT INTO cde_schedule (arrival_id, sequence, process_date)"
                + " SELECT ?, i, ? FROM generate_series(1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, runDate, total);
    }

    static void emit(final int maxPartitions, final LocalDate runDate, final Job job,
            final JobOperator operator, final JdbcTemplate jdbc) throws Exception {
        final String prefix = "DCRELANEP" + maxPartitions;
        seedLaneFixture(jdbc, prefix, runDate, maxPartitions);

        final JobExecution run = operator.start(job, new JobParametersBuilder()
                .addString("run.date", runDate.toString(), true)
                .addString("window", "lane" + maxPartitions, true).toJobParameters());
        assertEquals(BatchStatus.COMPLETED, run.getStatus());

        final long workers = run.getStepExecutions().stream()
                .filter(s -> s.getStepName().startsWith("emitWorkerStep:"))
                .count();
        if (maxPartitions == 1) {
            assertEquals(1, workers, "max-partitions=1 must collapse both clients into ONE lane");
        } else if (MULTI_CPU) {
            assertTrue(workers >= 2, "expected a real client-lane fan-out, got " + workers + " worker(s)");
        }

        // File set: 7tx parents split at max 3 into ordinal files _1.._3; 2tx parents keep
        // the bare legacy name (NO suffix ever on an unsplit parent).
        final Path rf = Path.of("build/test-exchange/fnbrf01/fint-req/out");
        final Path cc = Path.of("build/test-exchange/fnbcc01/fint-req/out");
        for (int i = 1; i <= 3; i++) {
            assertTrue(Files.exists(rf.resolve("FNBRF01_" + prefix + "RF2_" + i + "_PAIN008.xml")),
                    "split ordinal file " + i + " missing for FNBRF01");
            assertTrue(Files.exists(cc.resolve("FNBCC01_" + prefix + "CC2_" + i + "_PAIN008.xml")),
                    "split ordinal file " + i + " missing for FNBCC01");
        }
        assertTrue(Files.exists(rf.resolve("FNBRF01_" + prefix + "RF1_PAIN008.xml")),
                "unsplit parent must keep the bare legacy filename");
        assertTrue(Files.exists(cc.resolve("FNBCC01_" + prefix + "CC1_PAIN008.xml")),
                "unsplit parent must keep the bare legacy filename");

        // Insertion-order FIFO eligibility (Sean ruling 9): RF2 was inserted FIRST, so its
        // group must be created first even though RF1 wins on msg-id AND arrival-uuid order.
        final List<String> groupOrder = jdbc.queryForList(
                "SELECT source_msg_id FROM crw_emission_group WHERE client = 'FNBRF01'"
                        + " AND run_date = ? ORDER BY created_at", String.class, runDate);
        assertEquals(List.of(prefix + "RF2", prefix + "RF1"), groupOrder,
                "within a client, eligibility follows tx_header insertion order");

        // Normalized projection (msg-id suffix only, context prefix stripped): group rows +
        // ordinal batch rows with frozen totals, live member counts and states.
        final List<String> projection = jdbc.queryForList("""
                SELECT g.client || '|' || right(g.source_msg_id, 3) || '|GROUP|'
                       || g.expected_batch_count || '|' || g.total_tx || '|' || g.split
                FROM crw_emission_group g WHERE g.run_date = ?
                UNION ALL
                SELECT g.client || '|' || right(g.source_msg_id, 3) || '|' || e.batch_ordinal || '|'
                       || e.tx_count || '|'
                       || (SELECT count(*) FROM crw_emission_member m WHERE m.emission_id = e.id)
                       || '|' || e.state
                FROM crw_emission e JOIN crw_emission_group g ON g.id = e.group_id
                WHERE e.run_date = ?
                ORDER BY 1""", String.class, runDate, runDate);
        assertEquals(12, projection.size(), "4 groups + 8 ordinal batches");
        PROJECTIONS.put(maxPartitions, projection);
    }

    @Nested
    @SpringBootTest(properties = {"spring.batch.job.enabled=false",
            "dcre.exchange-root=build/test-exchange", "DCRE_EXCHANGE_ROOT=build/test-exchange",
            "dcre.crw.max-partitions=1", "dcre.crw.split.max-size=3"})
    class SequentialLanes {

        @DynamicPropertySource
        static void props(final DynamicPropertyRegistry registry) {
            registerDb(registry);
        }

        @Autowired Job crwJob;
        @Autowired JobOperator jobOperator;
        @Autowired JdbcTemplate jdbc;

        @Test
        void emitsFixture() throws Exception {
            emit(1, LocalDate.of(2026, 9, 1), crwJob, jobOperator, jdbc);
        }
    }

    @Nested
    @SpringBootTest(properties = {"spring.batch.job.enabled=false",
            "dcre.exchange-root=build/test-exchange", "DCRE_EXCHANGE_ROOT=build/test-exchange",
            "dcre.crw.max-partitions=5", "dcre.crw.split.max-size=3"})
    class PartitionedLanes {

        @DynamicPropertySource
        static void props(final DynamicPropertyRegistry registry) {
            registerDb(registry);
        }

        @Autowired Job crwJob;
        @Autowired JobOperator jobOperator;
        @Autowired JdbcTemplate jdbc;

        @Test
        void emitsFixture() throws Exception {
            emit(5, LocalDate.of(2026, 9, 2), crwJob, jobOperator, jdbc);
        }
    }
}
