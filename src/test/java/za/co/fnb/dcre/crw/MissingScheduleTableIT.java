package za.co.fnb.dcre.crw;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import za.co.fnb.dcre.crw.service.EmissionService;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A-76 (SCRUM-107), found by the chaos gate on 2026-08-07.
 *
 * <p>CDE owns {@code cde_schedule} and creates it on its first run, so on a freshly reset
 * database it does not exist. Every 10s CRW window then died with
 * {@code relation "cde_schedule" does not exist} (BackoffLimitExceeded, exit 5), burning the
 * relaunch budget and filling {@code stage_outcome} with TECH_FAILED, when the correct answer
 * is simply that nothing is due.
 *
 * <p>It stopped being cosmetic once R-37 was amended: DC {@code DAG_COMPLETE} now requires a CRW
 * emission, so a CRW that cannot run at all means no collections arrival can ever complete.
 *
 * <p>Bootstrap ordering is not failure. A dependency that has not bootstrapped yet is no work.
 */
class MissingScheduleTableIT extends CrwTestcontainersBase {

    @Autowired
    private EmissionService emissions;

    @AfterEach
    void restoreTheTable() {
        jdbc.execute("CREATE TABLE IF NOT EXISTS cde_schedule ("
                + "arrival_id UUID NOT NULL, sequence INT NOT NULL, process_date DATE NOT NULL,"
                + " PRIMARY KEY (arrival_id, sequence))");
    }

    @Test
    void anAbsentScheduleTableMeansNothingDueRatherThanAFailedWindow() {
        dropWithDependents("cde_schedule");

        final int emitted = assertDoesNotThrow(() -> emissions.emitDue(LocalDate.now()),
                "a CRW window must not fail because CDE has not bootstrapped yet: it burns the"
                        + " relaunch budget and, since R-37 was amended, blocks DC completion");
        assertEquals(0, emitted, "nothing is due when no schedule exists");
    }

    @Test
    void theClientScopedLaneDegradesTheSameWay() {
        dropWithDependents("cde_schedule");

        final int emitted = assertDoesNotThrow(
                () -> emissions.emitDue(LocalDate.now(), "FNBCC01"),
                "the per-lane overload takes the same due query and must degrade identically");
        assertEquals(0, emitted, "nothing is due for the lane either");
    }

    /**
     * The first cut of this fix guarded ONLY cde_schedule. This test caught that it was too
     * narrow: it died on validation_log, because the same due query joins three tables owned by
     * three different services and a fresh database has none of them. Dropping any ONE of them
     * must degrade the same way.
     */
    @Test
    void anyOneMissingPeerTableDegradesTheSameWay() {
        for (final String table : new String[]{"cde_schedule", "validation_log", "tx_header"}) {
            restoreAllPeerTables();
            dropWithDependents(table);
            assertEquals(0, assertDoesNotThrow(() -> emissions.emitDue(LocalDate.now()),
                            "a missing " + table + " must mean nothing due, not a failed window"),
                    "nothing is due without " + table);
        }
    }

    /**
     * CASCADE, because crw_emission_owed (SCRUM-107) is defined over these tables and CockroachDB
     * refuses to drop a table a view depends on. EmissionOwedViewIT recreates the view in its own
     * setup, so the two suites stay order-independent.
     */
    private void dropWithDependents(final String table) {
        jdbc.execute("DROP TABLE IF EXISTS " + table + " CASCADE");
    }

    private void restoreAllPeerTables() {
        restoreTheTable();
        jdbc.execute("CREATE TABLE IF NOT EXISTS validation_log ("
                + "arrival_id UUID NOT NULL, sequence INT NOT NULL, outcome VARCHAR(32) NOT NULL,"
                + " PRIMARY KEY (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_header ("
                + "arrival_id UUID PRIMARY KEY, initg_pty VARCHAR(16))");
    }
}
