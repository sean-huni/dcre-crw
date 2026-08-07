package za.co.fnb.dcre.crw.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import za.co.fnb.dcre.crw.CrwTestcontainersBase;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionRepo;

import java.time.LocalDate;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * A-78 (SCRUM-107), found on the live cluster on 2026-08-07: all nine col-crw windows died
 * {@code exit 5} with {@code relation "ais_verdict" does not exist}.
 *
 * <p>The due queries are a UNION of two arms. The DC arm reads {@code cde_schedule} (CDE's);
 * the pay arm reads {@code ais_verdict} (AIS's). PostgreSQL resolves EVERY relation named in a
 * statement, so a missing table on EITHER arm kills the WHOLE query, including the arm that
 * could have run.
 *
 * <p>A-76 guarded three tables and returned "nothing due" when any was absent. Widening that
 * guard to cover {@code ais_verdict} as well is the obvious fix and is WORSE than the crash:
 * a collections-only cluster never runs AIS, so {@code ais_verdict} never exists, and CRW would
 * silently stop emitting for DC forever while reporting a clean run. A crash is loud; a
 * permanent silent zero is not. Since R-37 was amended, either one blocks DC completion.
 *
 * <p>So the arms are composed rather than guarded as a block: an arm whose tables are absent is
 * left OUT of the SQL, and the other arm still runs.
 *
 * <p>No existing fixture could express this. {@code CrwTestcontainersBase.ensureSpineTables}
 * creates all five peer tables for every suite, so every test ran in a database where both arms
 * were always resolvable: a monoculture in exactly the dimension that carried the bug.
 */
class SingleArmBootstrapIT extends CrwTestcontainersBase {

    private static final LocalDate RUN_DATE = LocalDate.of(2026, 7, 21);

    /** Isolated date for the one test that actually PUBLISHES, so its count has one explanation. */
    private static final LocalDate EMIT_DATE = LocalDate.of(2026, 9, 15);

    @Autowired
    private CrwEmissionRepo emissions;

    @Autowired
    private EmissionService emissions0;

    /** Both arms restored, so suites stay order-independent whichever one this test dropped. */
    @AfterEach
    void restoreBothArms() {
        ensureSpineTables();
    }

    /**
     * The live failure. A collections-only database has no {@code ais_verdict}, and the DC
     * arrival due today must still be found and still be emitted.
     */
    @Test
    void theDcArmStillRunsWhenAisHasNeverRun() {
        final UUID arrival = UUID.randomUUID();
        final String msgId = "DCRERF2026072100000781";
        seedDueArrival(arrival, "FNBRF81", msgId, 3, RUN_DATE);
        dropCascade("ais_verdict");

        assertThatCode(() -> emissions.findDueArrivals(RUN_DATE))
                .as("a missing pay-arm table must not kill the DC arm: this is the exact"
                        + " exit-5 crash loop that burned every CRW relaunch budget")
                .doesNotThrowAnyException();

        assertThat(emissions.findDueArrivals(RUN_DATE))
                .as("the DC arrival is due today and AIS is irrelevant to it")
                .contains(new DueArrivalRow(arrival, "FNBRF81", msgId));
        assertThat(emissions.findDueArrivals(RUN_DATE, "FNBRF81"))
                .contains(new DueArrivalRow(arrival, "FNBRF81", msgId));
        assertThat(emissions.findDueClients(RUN_DATE)).contains("FNBRF81");
    }

    /**
     * The same failure through the service entry point, which is what the job actually calls.
     * Asserting only on the repo would leave the A-76 guard free to return an empty due-set and
     * still look green.
     */
    @Test
    void theServiceEmitsForTheDcArmWithNoAisVerdictTable() {
        // FNBCC02 has exchange dirs configured (publication needs them) and is used by no other
        // suite; EMIT_DATE is likewise unshared. Both deliberate: the whole assertion is a COUNT,
        // and a client or date another suite also seeds would leave two explanations for it.
        final UUID arrival = UUID.randomUUID();
        seedDueArrival(arrival, "FNBCC02", "DCRECC2026091500000782", 3, EMIT_DATE);
        dropCascade("ais_verdict");

        assertThat(emissions0.dueClients(EMIT_DATE))
                .as("dueClients is the partitioner's FIRST call; an empty universe here means"
                        + " the window does no work at all and reports success")
                .contains("FNBCC02");
        assertThat(emissions0.emitDue(EMIT_DATE, "FNBCC02"))
                .as("nothing about a missing AIS table makes this DC file undue. This is the"
                        + " site that found claimMembers, the FOURTH two-arm statement")
                .isEqualTo(1);
    }

    /**
     * The mirror case, which the blanket guard would also have got wrong: an ENDO-only database
     * never runs CDE, so {@code cde_schedule} never exists and the pay arm must still run.
     */
    @Test
    void thePayArmStillRunsWhenCdeHasNeverRun() {
        final UUID arrival = UUID.randomUUID();
        final String msgId = "DCRERF2026072100000783";
        seedPayArrival(arrival, "FNBRF83", msgId, 4, RUN_DATE);
        dropCascade("cde_schedule");

        assertThatCode(() -> emissions.findDueArrivals(RUN_DATE)).doesNotThrowAnyException();
        assertThat(emissions.findDueArrivals(RUN_DATE))
                .as("pay rows are due from ingest day and never had a cde_schedule row")
                .contains(new DueArrivalRow(arrival, "FNBRF83", msgId));
        assertThat(emissions.findDueClients(RUN_DATE)).contains("FNBRF83");
    }

    /**
     * Planning runs per arrival AFTER due detection and reads the same two arms, so guarding only
     * the due queries would move the crash three lines later instead of removing it.
     */
    @Test
    void planningAlsoSurvivesAMissingArmTable() {
        final UUID arrival = UUID.randomUUID();
        final String msgId = "DCRERF2026072100000784";
        seedDueArrival(arrival, "FNBRF84", msgId, 6, RUN_DATE);
        dropCascade("ais_verdict");

        assertThatCode(() -> emissions.planTotals(RUN_DATE, arrival)).doesNotThrowAnyException();
        assertThat(emissions.planTotals(RUN_DATE, arrival).totalTx()).isEqualTo(6);
        assertThatCode(() -> emissions.batchBoundaries(RUN_DATE, arrival, 2)).doesNotThrowAnyException();
        assertThat(emissions.batchBoundaries(RUN_DATE, arrival, 2)).containsExactly(2, 4, 6);
    }

    /**
     * With NEITHER arm resolvable there is genuinely nothing due, and that must be an empty
     * result rather than a crash: bootstrap ordering is not failure.
     */
    @Test
    void neitherArmMeansNothingDueRatherThanAFailedWindow() {
        dropCascade("ais_verdict");
        dropCascade("cde_schedule");

        assertThat(emissions0.dueClients(RUN_DATE)).isEmpty();
        assertThat(emissions0.emitDue(RUN_DATE)).isZero();
    }

    /** CASCADE, because crw_emission_owed is defined over these tables. */
    private void dropCascade(final String table) {
        jdbc.execute("DROP TABLE IF EXISTS " + table + " CASCADE");
    }

    private void seedPayArrival(final UUID arrival, final String client, final String msgId,
            final int total, final LocalDate ingestDate) {
        ensureSpineTables();
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, flow, created_at)"
                + " VALUES (?,?,?,'PAY',?::TIMESTAMPTZ)", arrival, msgId, client, ingestDate + " 08:00:00+00");
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, e2e, amount)"
                + " SELECT ?, i, 'E2E' || i::STRING, 10.00 FROM generate_series(1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, total);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome)"
                + " SELECT ?, i, 'PASS' FROM generate_series(1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, total);
        jdbc.update("INSERT INTO ais_verdict (arrival_id, sequence, action)"
                + " SELECT ?, i, 'CREATED' FROM generate_series(1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, total);
    }
}
