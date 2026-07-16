package za.co.fnb.dcre.crw.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import za.co.fnb.dcre.crw.CrwTestcontainersBase;
import za.co.fnb.dcre.crw.config.CrwSplitProperties;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionGroupRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionMemberRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionRepo;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SCRUM-55 Task 3: snapshot-consistent split plan + set-based ordinal member
 * claims. The plan freezes at first claim: config changes and restarts replay
 * the STORED plan, never a repartition (spec section 3/4).
 */
class SplitPlannerIT extends CrwTestcontainersBase {

    private static final LocalDate RUN_DATE = LocalDate.of(2026, 7, 16);

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
    private String msgId;

    @BeforeEach
    void setUp() {
        txTemplate = new TransactionTemplate(txManager);
        arrivalId = UUID.randomUUID();
        setMax(5000);
    }

    private void setMax(final int max) {
        planner = new SplitPlanner(groups, emissions, members, new CrwSplitProperties(max, Map.of()));
    }

    private DueArrivalRow dueArrival() {
        return new DueArrivalRow(arrivalId, "FNBRF01", msgId);
    }

    private int memberCount(final CrwEmissionEntity batch) {
        return jdbc.queryForObject("SELECT count(*) FROM crw_emission_member WHERE emission_id = ?",
                Integer.class, batch.getId());
    }

    @Test
    void plans12001TxAtMax5000IntoThreeOrdinalBatches() {
        msgId = "DCRERF2026071600000001";
        seedDueArrival(arrivalId, "FNBRF01", msgId, 12001, RUN_DATE);

        var batches = txTemplate.execute(s -> planner.planAndClaim(dueArrival(), RUN_DATE));

        assertThat(batches).extracting(CrwEmissionEntity::getBatchOrdinal).containsExactly(1, 2, 3);
        assertThat(batches).extracting(CrwEmissionEntity::getOutboundMsgId).containsExactly(
                "DCRERF2026071600000001_1", "DCRERF2026071600000001_2", "DCRERF2026071600000001_3");
        assertThat(memberCount(batches.get(0))).isEqualTo(5000);
        assertThat(memberCount(batches.get(2))).isEqualTo(2001);
        var group = groups.findByArrivalIdAndRunDate(arrivalId, RUN_DATE).orElseThrow();
        assertThat(group.getAppliedMax()).isEqualTo(5000);
        assertThat(group.getExpectedBatchCount()).isEqualTo(3);
    }

    @Test
    void unsplitKeepsBareSourceMsgIdAndLegacyFilename() {
        msgId = "DCRERF2026071600000002";
        seedDueArrival(arrivalId, "FNBRF01", msgId, 4999, RUN_DATE);

        var batches = txTemplate.execute(s -> planner.planAndClaim(dueArrival(), RUN_DATE));

        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).getOutboundMsgId()).isEqualTo("DCRERF2026071600000002"); // NO suffix
        assertThat(batches.get(0).getFileName()).isEqualTo("FNBRF01_DCRERF2026071600000002_PAIN008.xml");
    }

    @Test
    void configChangePlusRestartNeverRepartitions() {
        msgId = "DCRERF2026071600000003";
        seedDueArrival(arrivalId, "FNBRF01", msgId, 12001, RUN_DATE);

        txTemplate.execute(s -> planner.planAndClaim(dueArrival(), RUN_DATE));      // plan at max 5000
        setMax(2000);                                                               // config change
        var replay = txTemplate.execute(s -> planner.planAndClaim(dueArrival(), RUN_DATE));

        assertThat(replay).hasSize(3);                                              // stored plan wins
    }
}
