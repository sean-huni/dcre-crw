package za.co.fnb.dcre.crw.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import za.co.fnb.dcre.crw.CrwTestcontainersBase;
import za.co.fnb.dcre.crw.config.CrwSplitProperties;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionGroupRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionMemberRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionRepo;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SCRUM-69: ENDO = Payments, immediate. Pay-flow rows (tx_header.flow =
 * 'PAY') are CRW-eligible from ingest day with ZERO cde_schedule rows:
 * the pay arm requires an AIS verdict, validation PASS rows and
 * runDate >= ingest date. The DC arm keeps requiring cde_schedule, and
 * the (arrival_id, run_date, batch_ordinal) idempotency still guards
 * re-runs.
 */
class PayFlowEligibilityIT extends CrwTestcontainersBase {

    private static final LocalDate RUN_DATE = LocalDate.of(2026, 7, 21);

    @Autowired
    CrwEmissionGroupRepo groups;

    @Autowired
    CrwEmissionRepo emissions;

    @Autowired
    CrwEmissionMemberRepo members;

    @Autowired
    PlatformTransactionManager txManager;

    private TransactionTemplate txTemplate;
    private SplitPlanner planner;
    private UUID arrivalId;

    @BeforeEach
    void setUp() {
        txTemplate = new TransactionTemplate(txManager);
        planner = new SplitPlanner(groups, emissions, members, new CrwSplitProperties(5000, Map.of()));
        arrivalId = UUID.randomUUID();
    }

    /** PAY spine: complete AIS coverage, PASS rows, ZERO cde_schedule rows, ingest stamped on the given day. */
    private void seedPayArrival(final UUID arrival, final String client, final String msgId,
            final int total, final LocalDate ingestDate) {
        seedPayArrival(arrival, client, msgId, total, total, ingestDate);
    }

    /** Variant with explicit AIS verdict coverage (M1: verdicts may lag the PASS set mid-run). */
    private void seedPayArrival(final UUID arrival, final String client, final String msgId,
            final int total, final int verdicts, final LocalDate ingestDate) {
        ensureSpineTables();
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, flow, created_at)"
                + " VALUES (?,?,?,'PAY',?::TIMESTAMPTZ)", arrival, msgId, client, ingestDate + " 08:00:00+00");
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, e2e, amount)"
                + " SELECT ?, i, 'E2E' || i::STRING, 10.00 FROM generate_series(1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, total);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome)"
                + " SELECT ?, i, 'PASS' FROM generate_series(1, ?) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, total);
        if (verdicts > 0) {
            jdbc.update("INSERT INTO ais_verdict (arrival_id, sequence, action)"
                    + " SELECT ?, i, 'CREATED' FROM generate_series(1, ?) AS g(i)"
                    + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, verdicts);
        }
    }

    @Test
    void payArrivalEmitsOnIngestDayWithoutCdeSchedule() {
        final String msgId = "DCRERF2026072100000701";
        seedPayArrival(arrivalId, "FNBRF71", msgId, 5, RUN_DATE);
        final DueArrivalRow due = new DueArrivalRow(arrivalId, "FNBRF71", msgId);

        assertThat(emissions.findDueArrivals(RUN_DATE)).contains(due);
        assertThat(emissions.findDueArrivals(RUN_DATE, "FNBRF71")).contains(due);
        assertThat(emissions.findDueClients(RUN_DATE)).contains("FNBRF71");

        final var batches = txTemplate.execute(s -> planner.planAndClaim(due, RUN_DATE));
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).getOutboundMsgId()).isEqualTo(msgId);
        assertThat(batches.get(0).getTxCount()).isEqualTo(5);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM crw_emission_member WHERE emission_id = ?",
                Integer.class, batches.get(0).getId())).isEqualTo(5);
    }

    @Test
    void payReplayOnTheSameRunDateNeverDuplicatesTheEmission() {
        final String msgId = "DCRERF2026072100000702";
        seedPayArrival(arrivalId, "FNBRF72", msgId, 4, RUN_DATE);
        final DueArrivalRow due = new DueArrivalRow(arrivalId, "FNBRF72", msgId);

        txTemplate.execute(s -> planner.planAndClaim(due, RUN_DATE));
        final var replay = txTemplate.execute(s -> planner.planAndClaim(due, RUN_DATE));

        assertThat(replay).hasSize(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM crw_emission WHERE arrival_id = ? AND run_date = ?",
                Integer.class, arrivalId, RUN_DATE)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM crw_emission_member WHERE emission_id = ?",
                Integer.class, replay.get(0).getId())).isEqualTo(4);
    }

    @Test
    void payArrivalIsNotDueBeforeItsIngestDay() {
        final String msgId = "DCRERF2026072100000703";
        seedPayArrival(arrivalId, "FNBRF73", msgId, 3, RUN_DATE);

        assertThat(emissions.findDueArrivals(RUN_DATE.minusDays(1)))
                .noneMatch(row -> row.arrivalId().equals(arrivalId));
        assertThat(emissions.findDueClients(RUN_DATE.minusDays(1))).doesNotContain("FNBRF73");
    }

    @Test
    void payParentEmittedOnDayOneIsNotDueOnDayTwo() {
        // Review B1: without the earlier-run-date guard the parent re-lists on
        // day 2 and plans a duplicate suffixed emission of the same rows.
        final String msgId = "DCRERF2026072100000705";
        seedPayArrival(arrivalId, "FNBRF75", msgId, 3, RUN_DATE);
        final DueArrivalRow due = new DueArrivalRow(arrivalId, "FNBRF75", msgId);
        final LocalDate dayTwo = RUN_DATE.plusDays(1);

        final var dayOne = txTemplate.execute(s -> planner.planAndClaim(due, RUN_DATE));
        dayOne.forEach(batch -> emissions.markVisible(batch.getId()));

        assertThat(emissions.findDueArrivals(dayTwo)).noneMatch(row -> row.arrivalId().equals(arrivalId));
        assertThat(emissions.findDueArrivals(dayTwo, "FNBRF75")).isEmpty();
        assertThat(emissions.findDueClients(dayTwo)).doesNotContain("FNBRF75");

        final var dayTwoPlan = txTemplate.execute(s -> planner.planAndClaim(due, dayTwo));
        assertThat(dayTwoPlan).as("day-2 plan for a day-1-emitted pay parent must be a no-op").isEmpty();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM crw_emission WHERE arrival_id = ?",
                Integer.class, arrivalId)).isEqualTo(1);
    }

    @Test
    void crashShapedDayOnePlanResumesOnDayOneButNotOnDayTwo() {
        // Review B1 crash shape: day-1 rows exist non-VISIBLE (kill before
        // publication). Day-1 re-poll must still list the parent (resume);
        // day-2 poll must not (recovery re-runs run_date = day 1).
        final String msgId = "DCRERF2026072100000706";
        seedPayArrival(arrivalId, "FNBRF76", msgId, 3, RUN_DATE);
        final DueArrivalRow due = new DueArrivalRow(arrivalId, "FNBRF76", msgId);

        txTemplate.execute(s -> planner.planAndClaim(due, RUN_DATE));

        assertThat(jdbc.queryForObject("SELECT count(*) FROM crw_emission WHERE arrival_id = ?"
                + " AND state <> 'VISIBLE'", Integer.class, arrivalId)).isEqualTo(1);
        assertThat(emissions.findDueArrivals(RUN_DATE)).contains(due);
        assertThat(emissions.findDueArrivals(RUN_DATE.plusDays(1)))
                .noneMatch(row -> row.arrivalId().equals(arrivalId));
    }

    @Test
    void partialAisCoverageIsNotDueUntilTheLastVerdictLands() {
        // Review M1: AIS slice-commits verdicts mid-run; presence of SOME
        // verdict must never trigger emission. Due only once the verdict set
        // covers the PASS set.
        final String msgId = "DCRERF2026072100000707";
        seedPayArrival(arrivalId, "FNBRF77", msgId, 3, 2, RUN_DATE);
        final DueArrivalRow due = new DueArrivalRow(arrivalId, "FNBRF77", msgId);

        assertThat(emissions.findDueArrivals(RUN_DATE)).noneMatch(row -> row.arrivalId().equals(arrivalId));
        assertThat(emissions.findDueClients(RUN_DATE)).doesNotContain("FNBRF77");

        jdbc.update("INSERT INTO ais_verdict (arrival_id, sequence, action) VALUES (?, 3, 'CREATED')"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrivalId);
        assertThat(emissions.findDueArrivals(RUN_DATE)).contains(due);
    }

    @Test
    void payParentWithoutAnyAisVerdictIsNotDue() {
        // Review m4: an AIS that never ran (or died before its first slice)
        // stays fail-closed.
        final String msgId = "DCRERF2026072100000708";
        seedPayArrival(arrivalId, "FNBRF78", msgId, 3, 0, RUN_DATE);

        assertThat(emissions.findDueArrivals(RUN_DATE)).noneMatch(row -> row.arrivalId().equals(arrivalId));
        assertThat(emissions.findDueClients(RUN_DATE)).doesNotContain("FNBRF78");
    }

    @Test
    void payParentWithZeroPassRowsIsNotDueAndPlansEmpty() {
        // Review m4: nothing validated PASS means nothing to emit, even with
        // full AIS coverage of an empty PASS set.
        final String msgId = "DCRERF2026072100000709";
        ensureSpineTables();
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, flow, created_at)"
                + " VALUES (?,?,?,'PAY',?::TIMESTAMPTZ)", arrivalId, msgId, "FNBRF79", RUN_DATE + " 08:00:00+00");
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, e2e, amount)"
                + " SELECT ?, i, 'E2E' || i::STRING, 10.00 FROM generate_series(1, 3) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrivalId);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome)"
                + " SELECT ?, i, 'FAIL' FROM generate_series(1, 3) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrivalId);
        jdbc.update("INSERT INTO ais_verdict (arrival_id, sequence, action)"
                + " SELECT ?, i, 'CREATED' FROM generate_series(1, 3) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrivalId);

        assertThat(emissions.findDueArrivals(RUN_DATE)).noneMatch(row -> row.arrivalId().equals(arrivalId));
        final var plan = txTemplate.execute(s ->
                planner.planAndClaim(new DueArrivalRow(arrivalId, "FNBRF79", msgId), RUN_DATE));
        assertThat(plan).isEmpty();
    }

    @Test
    void dcArrivalWithoutCdeScheduleStaysIneligible() {
        final String msgId = "DCRERF2026072100000704";
        ensureSpineTables();
        // Same rows a pay arrival gets, but flow stays COL: eligibility must
        // hinge on the flow, never on verdict/PASS presence alone.
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, created_at)"
                + " VALUES (?,?,?,?::TIMESTAMPTZ)", arrivalId, msgId, "FNBRF74", RUN_DATE + " 08:00:00+00");
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, e2e, amount)"
                + " SELECT ?, i, 'E2E' || i::STRING, 10.00 FROM generate_series(1, 3) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrivalId);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome)"
                + " SELECT ?, i, 'PASS' FROM generate_series(1, 3) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrivalId);
        jdbc.update("INSERT INTO ais_verdict (arrival_id, sequence, action)"
                + " SELECT ?, i, 'CREATED' FROM generate_series(1, 3) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrivalId);

        assertThat(emissions.findDueArrivals(RUN_DATE)).noneMatch(row -> row.arrivalId().equals(arrivalId));
        assertThat(emissions.findDueClients(RUN_DATE)).doesNotContain("FNBRF74");
    }
}
