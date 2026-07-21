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

    /** PAY spine: AIS verdict + PASS rows, ZERO cde_schedule rows, ingest stamped on the given day. */
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
