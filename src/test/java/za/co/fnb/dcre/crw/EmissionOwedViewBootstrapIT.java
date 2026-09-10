package za.co.fnb.dcre.crw;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A-79 (SCRUM-107), found on the live cluster on 2026-08-07.
 *
 * <p>{@code crw_emission_owed} is the PUBLISHED contract AGT gates DC {@code DAG_COMPLETE} on
 * (R-37 as amended). Its changeset guarded its four peer tables with
 * {@code onFail="MARK_RAN"}, intending "CRW's isolated test database has none of them, so skip".
 *
 * <p>MARK_RAN does not mean skip. It means RECORD AS APPLIED, PERMANENTLY. CRW's window job runs
 * every 10 seconds from cluster start, so it reaches Liquibase long before the first DC arrival
 * makes CDE create {@code cde_schedule}. The changeset therefore MARK_RANs on the REAL shared
 * database too, on the very first window, and the view is never created for the life of that
 * database. Observed on dcre_col:
 *
 * <pre>
 * id                          filename                                          exectype
 * 001-crw-emission-owed-view  db/changelog/2026/08/001-emission-owed-view.xml    MARK_RAN
 * </pre>
 *
 * <p>The v1 baseline carries the fixed changeset at
 * {@code db/changelog/2026/08/003-emission-owed-view.xml}; the ids and filenames above are the
 * pre-v1 ones the defect was observed under.
 *
 * <p>AGT's {@code CollectionsReadRepo.emissionOwedFor} fails closed to OWED on a structural
 * error (42P01), which is the correct direction and turns this into a silent permanent hang:
 * every collections arrival sits in {@code DAG_RUNNING} forever, with no error anywhere.
 *
 * <p>A dependency that has not bootstrapped yet is NOT work already done. The retryable skip is
 * {@code onFail="CONTINUE"}: skip this run, record nothing, re-evaluate next startup.
 *
 * <p>Nothing caught this because {@code CrwTestcontainersBase.ensureSpineTables} creates every
 * peer table for every other suite, so no fixture in the repo could express a database where
 * they are absent. This one runs Liquibase itself, against its own empty database.
 */
class EmissionOwedViewBootstrapIT extends CrwTestcontainersBase {

    private static final String BOOTSTRAP_DB = "crw_a79_bootstrap";

    private static final String CHANGELOG = "db/changelog/db.changelog-master.xml";

    /**
     * The peer tables the view is defined over, minus CRW's own: these are exactly the ones a
     * fresh database does not have until CRR, CTV and CDE have each run at least once.
     */
    private static final String[] PEER_DDL = {
            "CREATE TABLE IF NOT EXISTS tx_header (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                    + " arrival_id UUID UNIQUE, msg_id VARCHAR(35), initg_pty VARCHAR(35),"
                    + " created_at TIMESTAMPTZ NOT NULL DEFAULT now())",
            "CREATE TABLE IF NOT EXISTS validation_log (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                    + " arrival_id UUID, sequence INT, outcome VARCHAR(32), UNIQUE (arrival_id, sequence))",
            "CREATE TABLE IF NOT EXISTS cde_schedule (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                    + " arrival_id UUID, sequence INT, process_date DATE, UNIQUE (arrival_id, sequence))",
    };

    /**
     * The whole defect in one run: migrate before the peers exist, then migrate again after they
     * appear, and require the view to be there. Under MARK_RAN the second migration is a no-op
     * and the view never exists.
     */
    @Test
    void theViewAppearsOnTheFirstMigrationAfterItsPeerTablesExist() throws Exception {
        jdbc.execute("DROP DATABASE IF EXISTS " + BOOTSTRAP_DB + " CASCADE");
        jdbc.execute("CREATE DATABASE " + BOOTSTRAP_DB);
        final String url = bootstrapUrl();

        migrate(url);
        assertFalse(viewExists(url),
                "control: with no cde_schedule the view cannot be created, so a false PASS here"
                        + " would mean the probe is not looking at the bootstrap database at all");
        assertTrue(tableExists(url, "crw_emission"),
                "control: CRW's OWN tables must have migrated, otherwise this test proves nothing"
                        + " about the view and only that Liquibase did not run");

        for (final String ddl : PEER_DDL) {
            execute(url, ddl);
        }

        migrate(url);
        assertTrue(viewExists(url),
                "the peer tables now exist, so the next migration must create crw_emission_owed."
                        + " MARK_RAN recorded the first skip as applied, so the changeset is never"
                        + " reconsidered and every DC arrival hangs in DAG_RUNNING forever");
    }

    /**
     * The skip must leave NO history row. A recorded row is what makes it permanent, so asserting
     * only on the view would still pass for a fix that merely reran the DDL by another route.
     */
    @Test
    void theSkippedRunRecordsNoHistoryRowForTheViewChangeset() throws Exception {
        jdbc.execute("DROP DATABASE IF EXISTS " + BOOTSTRAP_DB + "_hist CASCADE");
        jdbc.execute("CREATE DATABASE " + BOOTSTRAP_DB + "_hist");
        final String url = bootstrapUrl().replace(BOOTSTRAP_DB, BOOTSTRAP_DB + "_hist");

        migrate(url);

        assertEquals(0, countViewChangesetRows(url),
                "a dependency that has not bootstrapped yet must record nothing: any row here,"
                        + " MARK_RAN included, means the changeset will never be reconsidered");
    }

    private String bootstrapUrl() {
        // CockroachContainer hands back .../<db>?params; swap only the path segment.
        return CRDB.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + BOOTSTRAP_DB + "$1");
    }

    private void migrate(final String url) throws Exception {
        try (Connection c = open(url)) {
            final Database db = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(c));
            db.setDatabaseChangeLogTableName("crw_databasechangelog");
            db.setDatabaseChangeLogLockTableName("crw_databasechangeloglock");
            try (Liquibase liquibase = new Liquibase(CHANGELOG, new ClassLoaderResourceAccessor(), db)) {
                liquibase.update(new Contexts(), new LabelExpression());
            }
        }
    }

    private Connection open(final String url) throws Exception {
        return DriverManager.getConnection(url, CRDB.getUsername(), CRDB.getPassword());
    }

    private void execute(final String url, final String sql) throws Exception {
        try (Connection c = open(url); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    private boolean viewExists(final String url) throws Exception {
        return scalar(url, "SELECT count(*) FROM information_schema.views"
                + " WHERE table_schema = 'public' AND table_name = 'crw_emission_owed'") > 0;
    }

    private boolean tableExists(final String url, final String table) throws Exception {
        return scalar(url, "SELECT count(*) FROM information_schema.tables"
                + " WHERE table_schema = 'public' AND table_name = '" + table + "'") > 0;
    }

    private long countViewChangesetRows(final String url) throws Exception {
        return scalar(url, "SELECT count(*) FROM crw_databasechangelog"
                + " WHERE filename LIKE '%emission-owed-view%'");
    }

    private long scalar(final String url, final String sql) throws Exception {
        try (Connection c = open(url); Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
