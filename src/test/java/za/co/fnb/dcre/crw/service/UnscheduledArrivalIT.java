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
 * CDE's schedule is the ONLY thing that makes a collections arrival due, and this is the case
 * that says so.
 *
 * <p>It was carried out of {@code PayFlowEligibilityIT} when the payments lane was deleted. In
 * that suite it was the control: the same rows a pay arrival got, with the flow left at COL, to
 * prove eligibility hinged on the flow rather than on PASS rows alone. The flow discriminator is
 * gone, but the invariant it was controlling for is not, and nothing else in the repo asserts it
 * directly: every other fixture seeds cde_schedule, so a due query that dropped the schedule join
 * and keyed on validation PASS alone would leave them all green while emitting for arrivals CDE
 * has not scheduled yet.
 *
 * <p>The emission is what makes it matter rather than a curiosity. crw_emission_owed says an
 * arrival with PASS rows and no plan is still OWED, so if CRW emitted here it would emit BEFORE
 * CDE has decided which process date the rows belong to, and the file would carry rows scheduled
 * for a date that has not arrived.
 */
class UnscheduledArrivalIT extends CrwTestcontainersBase {

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

    @Test
    void anArrivalWithPassRowsButNoScheduleIsNeitherDueNorPlannable() {
        final String msgId = "DCRECC2026072100000704";
        ensureSpineTables();
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, created_at)"
                + " VALUES (?,?,?,?::TIMESTAMPTZ)", arrivalId, msgId, "FNBRF74", RUN_DATE + " 08:00:00+00");
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, e2e, amount)"
                + " SELECT ?, i, 'E2E' || i::STRING, 10.00 FROM generate_series(1, 3) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrivalId);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome)"
                + " SELECT ?, i, 'PASS' FROM generate_series(1, 3) AS g(i)"
                + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrivalId);
        // No cde_schedule rows: CTV has validated, CDE has not scheduled.

        assertThat(emissions.findDueArrivals(RUN_DATE))
                .as("PASS rows alone must never make an arrival due: only a cde_schedule row for"
                        + " the run date does")
                .noneMatch(row -> row.arrivalId().equals(arrivalId));
        assertThat(emissions.findDueArrivals(RUN_DATE, "FNBRF74")).isEmpty();
        assertThat(emissions.findDueClients(RUN_DATE)).doesNotContain("FNBRF74");

        // Planning is the second gate: reached directly it must also decline, so a caller that
        // obtained the arrival by some other route still cannot emit an unscheduled file.
        final var plan = txTemplate.execute(s ->
                planner.planAndClaim(new DueArrivalRow(arrivalId, "FNBRF74", msgId), RUN_DATE));
        assertThat(plan).as("nothing scheduled means nothing to freeze into a batch").isEmpty();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM crw_emission WHERE arrival_id = ?",
                Integer.class, arrivalId)).isZero();
    }
}
