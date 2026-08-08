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
     * Cross-service spine tables the CRW queries read: tx_header and tx_entry
     * (CRR's), validation_log (CTV's) and cde_schedule (CDE's). SINGLE DDL
     * source for every crw test context (review m3): CrwJobTest, the BDD glue
     * and the lane IT call this static against their own datasource.
     *
     * <p>No flow column and no ais_verdict table. Both existed for the
     * payments lane, which is PRW's; tx_header.flow is being deleted
     * fleet-wide because it existed only to let two bounded contexts share one
     * table, and no changelog in the estate ever created ais_verdict.
     *
     * <p>{@code client_token} IS here, at CRR's shape and nullability (A-43): it is the
     * outbound client authority CRW now reads. Its absence was worse than a fixture
     * monoculture, because a fixture that omits the column the shipped SQL names cannot
     * express the behaviour in either direction.
     */
    public static void ensureSpineTables(final JdbcTemplate jdbc) {
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_header (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID UNIQUE, msg_id VARCHAR(35), initg_pty VARCHAR(35),"
                + " client_token VARCHAR(16),"
                + " created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_entry (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, e2e VARCHAR(35), amount DECIMAL(18,2), UNIQUE (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS validation_log (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, outcome VARCHAR(32), UNIQUE (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS cde_schedule (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, process_date DATE, UNIQUE (arrival_id, sequence))");
    }

    /** Instance convenience over the shared static DDL source. */
    protected void ensureSpineTables() {
        ensureSpineTables(jdbc);
    }

    /**
     * Spine fixture mirroring CrwJobTest's seed, set-based for the split
     * scale (12k+ rows): every sequence 1..total is PASS-validated and due
     * on the run date. generate_series keeps the seed at 3 statements.
     */
    protected void seedDueArrival(final UUID arrival, final String client, final String msgId,
            final int total, final LocalDate runDate) {
        seedDueArrival(arrival, client, client, msgId, total, runDate);
    }

    /**
     * The same fixture with the two client homes stated SEPARATELY, so a test can express
     * an arrival whose R-31 filename token and copybook {@code destination_id} differ, or
     * one that carries no filename token at all.
     *
     * <p>Every other fixture in this suite seeds both columns to the same value, which
     * makes them structurally incapable of seeing which one CRW reads: the change from
     * {@code initg_pty} to {@code client_token} would pass them whether it were applied
     * correctly, incorrectly or not at all. {@code ClientAuthorityIT} is the only caller
     * that can, and it is the only reason this overload exists.
     *
     * @param clientToken the R-31 filename token; {@code null} models a job launched
     *                    outside AGT, where {@code initg_pty} must carry the emission
     * @param initgPty    the copybook {@code destination_id}, never null in production
     */
    protected void seedDueArrival(final UUID arrival, final String clientToken, final String initgPty,
            final String msgId, final int total, final LocalDate runDate) {
        ensureSpineTables();
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, client_token) VALUES (?,?,?,?)",
                arrival, msgId, initgPty, clientToken);
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
