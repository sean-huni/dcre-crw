package za.co.fnb.dcre.crw;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-107: executes the SHIPPED {@code crw_emission_owed} SQL against a real
 * database. AGT gates DAG_COMPLETE on this view, so a wrong column or predicate
 * here does not fail loudly, it silently holds every collections arrival open
 * forever, indistinguishable from legitimate warehousing.
 *
 * <p>The view body is READ OUT OF THE CHANGESET rather than copied here. A copy
 * would be a second home for one fact, and the copy is the one that stays right
 * while the shipped SQL drifts. The changeset itself MARK_RANs in this repo's
 * migration (it needs four tables owned by other services), so this test creates
 * those tables and then applies the very same view definition.
 *
 * <p>The negative case that matters most is {@code MATERIALIZED}: a planned but
 * unpublished batch. Only that assertion catches a predicate that tests for row
 * existence instead of publication state.
 */
class EmissionOwedViewIT extends CrwTestcontainersBase {

    private static final String CHANGESET = "db/changelog/2026/08/001-emission-owed-view.xml";

    /** The view SELECT exactly as it ships, pulled from the changeset's createView body. */
    private static String shippedViewBody() throws Exception {
        try (InputStream in = EmissionOwedViewIT.class.getClassLoader().getResourceAsStream(CHANGESET)) {
            assertNotNull(in, "changeset not on the classpath: " + CHANGESET);
            final var doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(in);
            final var nodes = doc.getElementsByTagName("createView");
            assertTrue(nodes.getLength() > 0, "no createView in " + CHANGESET);
            return nodes.item(0).getTextContent();
        }
    }

    @BeforeEach
    void buildTheForeignTablesAndTheShippedView() throws Exception {
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_header ("
                + "arrival_id UUID PRIMARY KEY, initg_pty VARCHAR(16))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS validation_log ("
                + "arrival_id UUID NOT NULL, sequence INT NOT NULL, outcome VARCHAR(16) NOT NULL,"
                + " PRIMARY KEY (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS cde_schedule ("
                + "arrival_id UUID NOT NULL, sequence INT NOT NULL, process_date DATE NOT NULL,"
                + " PRIMARY KEY (arrival_id, sequence))");
        jdbc.execute("DROP VIEW IF EXISTS crw_emission_owed");
        jdbc.execute("CREATE VIEW crw_emission_owed AS " + shippedViewBody());
        jdbc.execute("DELETE FROM crw_emission_member");
        jdbc.execute("DELETE FROM crw_emission");
        jdbc.execute("DELETE FROM crw_emission_group");
        jdbc.execute("DELETE FROM cde_schedule");
        jdbc.execute("DELETE FROM validation_log");
        jdbc.execute("DELETE FROM tx_header");
    }

    private boolean owed(final UUID arrival) {
        final Boolean b = jdbc.queryForObject(
                "SELECT owed FROM crw_emission_owed WHERE arrival_id = ?", Boolean.class, arrival);
        assertNotNull(b, "the arrival must appear in the view");
        return b;
    }

    /** An arrival whose rows all PASS and are all due TODAY, so only publication is outstanding. */
    private UUID arrivalDueTodayWithPassRows(final int rows) {
        final UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO tx_header (arrival_id, initg_pty) VALUES (?, ?)", id, "FNBCC01");
        for (int i = 1; i <= rows; i++) {
            jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,'PASS')", id, i);
            jdbc.update("INSERT INTO cde_schedule (arrival_id, sequence, process_date)"
                    + " VALUES (?,?,current_date)", id, i);
        }
        return id;
    }

    private void plan(final UUID arrival, final int expectedBatches) {
        jdbc.update("INSERT INTO crw_emission_group (id, arrival_id, client, source_msg_id, run_date,"
                        + " applied_max, total_tx, total_amount, expected_batch_count, split)"
                        + " VALUES (?,?,?,?,current_date,5000,10,1.00,?,?)",
                UUID.randomUUID(), arrival, "FNBCC01", "MSG-" + arrival.toString().substring(0, 8),
                expectedBatches, expectedBatches > 1);
    }

    private void batch(final UUID arrival, final int ordinal, final String state) {
        jdbc.update("INSERT INTO crw_emission (id, arrival_id, run_date, batch_ordinal, file_name, state)"
                        + " VALUES (?,?,current_date,?,?,?)",
                UUID.randomUUID(), arrival, ordinal, "F" + ordinal + ".xml", state);
    }

    @Test
    void aPlannedButUnpublishedBatchIsStillOwed() {
        final UUID a = arrivalDueTodayWithPassRows(3);
        plan(a, 1);
        batch(a, 1, "MATERIALIZED");
        assertTrue(owed(a), "MATERIALIZED is planned, not published: the file has not reached Fintegrate."
                + " A predicate testing row existence rather than state passes here and must not.");
    }

    @Test
    void everyPlannedBatchMustBeVisible() {
        final UUID a = arrivalDueTodayWithPassRows(3);
        plan(a, 3);
        batch(a, 1, "VISIBLE");
        assertTrue(owed(a), "1 of 3 published: an any-row-visible predicate completes here with"
                + " two batches undelivered");
        batch(a, 2, "VISIBLE");
        assertTrue(owed(a), "2 of 3 published");
        batch(a, 3, "VISIBLE");
        assertFalse(owed(a), "all 3 published: nothing owed");
    }

    @Test
    void futuredRowsKeepTheArrivalOwedEvenWhenTodayIsFullyPublished() {
        final UUID a = UUID.randomUUID();
        jdbc.update("INSERT INTO tx_header (arrival_id, initg_pty) VALUES (?,?)", a, "FNBCC01");
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,1,'PASS')", a);
        jdbc.update("INSERT INTO cde_schedule (arrival_id, sequence, process_date) VALUES (?,1,current_date)", a);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,2,'PASS')", a);
        jdbc.update("INSERT INTO cde_schedule (arrival_id, sequence, process_date)"
                + " VALUES (?,2,current_date + INTERVAL '7 days')", a);
        plan(a, 1);
        batch(a, 1, "VISIBLE");
        assertTrue(owed(a), "today's batch is out but a futured row is still warehoused:"
                + " completing here is the R-37 case the gate exists to protect");
    }

    @Test
    void anArrivalWithZeroPassRowsOwesNothingAndMustNotHang() {
        final UUID a = UUID.randomUUID();
        jdbc.update("INSERT INTO tx_header (arrival_id, initg_pty) VALUES (?,?)", a, "FNBCC01");
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,1,'FAIL')", a);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,2,'FAIL')", a);
        assertFalse(owed(a), "CRW plans empty for this arrival forever; owing an emission that can"
                + " never happen strands it in DAG_RUNNING permanently");
    }

    @Test
    void anArrivalWithPassRowsButNoPlanYetIsOwed() {
        final UUID a = arrivalDueTodayWithPassRows(2);
        assertTrue(owed(a), "due today, PASS rows, CRW has not planned yet: still owed");
    }
}
