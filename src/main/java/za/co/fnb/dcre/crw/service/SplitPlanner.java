package za.co.fnb.dcre.crw.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import za.co.fnb.dcre.crw.config.CrwSplitProperties;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.CrwEmissionGroupEntity;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.model.PlanTotals;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionGroupRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionMemberRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionRepo;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Business tier: forms the SCRUM-55 split plan inside the caller's arrival
 * transaction (one CRDB snapshot) and claims group, batches and members
 * set-based. Claim-once at every level: a replay finds the stored plan and
 * returns it; a config change never repartitions an existing plan. The plan
 * transaction is publication-free: batches end MATERIALIZED and files are
 * written only after it commits (durable-effect ordering).
 */
@Service
public class SplitPlanner {

    private final CrwEmissionGroupRepo groups;
    private final CrwEmissionRepo emissions;
    private final CrwEmissionMemberRepo members;
    private final CrwSplitProperties split;

    public SplitPlanner(final CrwEmissionGroupRepo groups, final CrwEmissionRepo emissions,
                        final CrwEmissionMemberRepo members, final CrwSplitProperties split) {
        this.groups = groups;
        this.emissions = emissions;
        this.members = members;
        this.split = split;
    }

    /**
     * Inside the caller's arrival tx. Claim-once: a replay finds the stored
     * plan and returns it. The returned rows carry state and frozen identity
     * (outbound MsgId, file name) read in the SAME snapshot that computed the
     * prior-artifact offset (crw-5): the caller publishes the unpublished
     * batches from exactly these rows after commit and never recomputes
     * identity from a separate committed-rows-at-large read.
     */
    public List<CrwEmissionEntity> planAndClaim(final DueArrivalRow arrival, final LocalDate runDate) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException(
                    "planAndClaim must run inside the caller's arrival transaction (snapshot consistency)");
        }
        UUID arrivalId = arrival.arrivalId();
        var existing = groups.findByArrivalIdAndRunDate(arrivalId, runDate);
        if (existing.isEmpty()) {
            int max = split.maxFor(arrival.client());
            PlanTotals totals = emissions.planTotals(runDate, arrivalId);
            if (totals.totalTx() == 0) {
                return List.of();
            }
            int count = (int) Math.ceil(totals.totalTx() / (double) max);
            groups.claimPlan(CrwEmissionGroupEntity.planned(arrivalId, arrival.client(), arrival.msgId(),
                    runDate, max, totals.totalTx(), totals.totalAmount(), count, count > 1));
        }
        var group = groups.findByArrivalIdAndRunDate(arrivalId, runDate).orElseThrow();
        claimBatches(group, runDate);
        return emissions.findByArrivalIdAndRunDateOrderByBatchOrdinal(arrivalId, runDate);
    }

    /**
     * Cross-run-date identity ruling (SCRUM-55 review BLOCKER fix): the
     * outbound identity enumerates the parent's PHYSICAL ARTIFACTS across
     * ALL run dates. The parent's first plan keeps the ratified rules
     * exactly (unsplit = bare source MsgId, split = _1.._N); a re-emission
     * (warehoused rows maturing on a later run date) continues the artifact
     * sequence and is ALWAYS suffixed: reusing the bare MsgId or a prior
     * ordinal would be a duplicate MsgId on the wire or a StagedWrite
     * restart no-op silently dropping the matured collections. Bare and _1
     * never coexist within a parent, so the identity space stays
     * collision-free (idempotency key = FULL identity, engineering canon).
     */
    private void claimBatches(final CrwEmissionGroupEntity g, final LocalDate runDate) {
        List<Integer> bounds = g.getExpectedBatchCount() > 1
                ? emissions.batchBoundaries(runDate, g.getArrivalId(), g.getAppliedMax()) : List.of();
        long prior = emissions.countPriorArtifacts(g.getArrivalId(), runDate);
        boolean suffixed = g.isSplit() || prior > 0;
        int lo = 0; // exclusive lower bound sequence; first batch starts at the beginning
        for (int ordinal = 1; ordinal <= g.getExpectedBatchCount(); ordinal++) {
            int hi = ordinal <= bounds.size() ? bounds.get(ordinal - 1) : -1; // -1 = open-ended tail
            long artifact = prior + ordinal;
            String outbound = suffixed
                    ? "%s_%d".formatted(g.getSourceMsgId(), artifact) : g.getSourceMsgId();
            String file = suffixed
                    ? "%s_%s_%d_PAIN008.xml".formatted(g.getClient(), g.getSourceMsgId(), artifact)
                    : "%s_%s_PAIN008.xml".formatted(g.getClient(), g.getSourceMsgId());
            emissions.claimSnapshot(CrwEmissionEntity.plannedBatch(g.getId(), g.getArrivalId(),
                    runDate, ordinal, outbound, file));
            var batch = emissions.findByArrivalIdAndRunDateOrderByBatchOrdinal(g.getArrivalId(), runDate)
                    .get(ordinal - 1);
            if ("PLANNED".equals(batch.getState())) {
                members.claimMembers(batch.getId(), runDate, g.getArrivalId(), lo + 1, hi);
                var totals = members.batchTotals(batch.getId());
                emissions.freezeTotals(batch.getId(), totals.count(), totals.sum());
                // MATERIALIZED is the plan tx's terminal state: members and totals
                // frozen, NO file yet. Publication (StagedWrite then markVisible)
                // happens only AFTER this transaction commits (SCRUM-55 review
                // fix, durable-effect ordering).
                emissions.transition(batch.getId(), "MATERIALIZED");
            }
            lo = hi == -1 ? lo : hi;
        }
    }
}
