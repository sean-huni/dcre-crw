package za.co.fnb.dcre.crw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Shared Testcontainers base for the SCRUM-55 split ITs: one CockroachDB
 * container + one Spring context (context caching) across every subclass,
 * bootstrapped exactly like CrwJobTest (Liquibase on, batch schema never
 * initialized, job auto-run off, files under build/test-exchange).
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
public abstract class CrwTestcontainersBase {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(final DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    @Autowired
    protected JdbcTemplate jdbc;

    /**
     * Cross-service spine tables the CRW queries read (CRR/AIS owned in
     * production): tx_header carries flow (SCRUM-69, default COL) and
     * ais_verdict backs the pay-flow eligibility arm.
     */
    protected void ensureSpineTables() {
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

    /**
     * Spine fixture mirroring CrwJobTest's seed, set-based for the split
     * scale (12k+ rows): every sequence 1..total is PASS-validated and due
     * on the run date. generate_series keeps the seed at 3 statements.
     */
    protected void seedDueArrival(final UUID arrival, final String client, final String msgId,
            final int total, final LocalDate runDate) {
        ensureSpineTables();
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty) VALUES (?,?,?)", arrival, msgId, client);
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, e2e, amount)"
                + " SELECT ?, i, 'E2E' || i::STRING, 10.00 FROM generate_series(1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, total);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome)"
                + " SELECT ?, i, 'PASS' FROM generate_series(1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, total);
        jdbc.update("INSERT INTO cde_schedule (arrival_id, sequence, process_date)"
                + " SELECT ?, i, ? FROM generate_series(1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, runDate, total);
    }

    /**
     * Two-date warehousing fixture (R-37, the canonical CrwJobTest.seed shape
     * at set-based scale): sequences 1..dueNow are due on dayOne; the
     * remaining futured sequences mature on dayTwo, so the SAME arrival
     * legitimately becomes due AGAIN on the later run date. Regression
     * fixture for the cross-run-date outbound-identity collision
     * (SCRUM-55 review fix, idempotency-key completeness).
     */
    protected void seedTwoDateArrival(final UUID arrival, final String client, final String msgId,
            final int dueNow, final int futured, final LocalDate dayOne, final LocalDate dayTwo) {
        seedDueArrival(arrival, client, msgId, dueNow, dayOne);
        final int total = dueNow + futured;
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, e2e, amount)"
                + " SELECT ?, i, 'E2E' || i::STRING, 10.00 FROM generate_series(? + 1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, dueNow, total);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome)"
                + " SELECT ?, i, 'PASS' FROM generate_series(? + 1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, dueNow, total);
        jdbc.update("INSERT INTO cde_schedule (arrival_id, sequence, process_date)"
                + " SELECT ?, i, ? FROM generate_series(? + 1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, dayTwo, dueNow, total);
    }
}
