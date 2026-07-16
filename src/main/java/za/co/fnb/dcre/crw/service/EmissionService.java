package za.co.fnb.dcre.crw.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.CrwEmissionMemberEntity;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.model.DueRow;
import za.co.fnb.dcre.crw.data.model.FuturedCountRow;
import za.co.fnb.dcre.crw.data.model.FuturedRow;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionMemberRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionRepo;
import za.co.fnb.dcre.platform.files.ExchangeChannel;
import za.co.fnb.dcre.platform.files.ExchangeLayout;
import za.co.fnb.dcre.platform.files.ExchangeSub;
import za.co.fnb.dcre.platform.files.StagedWrite;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Business tier: the R-37 Process-Date Executor. Emits pain.008 ONLY for
 * transactions whose process_date equals the run date; futured work stays
 * warehoused. Snapshot-first per R-24: membership is claimed immutably
 * BEFORE any file is built, so a restart reuses the identical member set
 * even when the schedule has moved on. StagedWrite = restart no-op (R-05).
 */
@Service
public class EmissionService {

    private static final Logger log = LoggerFactory.getLogger(EmissionService.class);

    /** Above this, a futured (arrival, process date) group logs ONE summary WARN instead of per-tx lines. */
    static final long FUTURED_DETAIL_WARN_LIMIT = 100;

    private final CrwEmissionRepo emissions;
    private final CrwEmissionMemberRepo members;
    private final Pain008Writer painWriter;
    private final ExchangeLayout layout;
    private final TransactionTemplate arrivalTx;

    public EmissionService(final CrwEmissionRepo emissions, final CrwEmissionMemberRepo members,
                           final Pain008Writer painWriter, final ExchangeLayout layout,
                           final PlatformTransactionManager txManager) {
        this.emissions = emissions;
        this.members = members;
        this.painWriter = painWriter;
        this.layout = layout;
        // SCRUM-42 load fix: each arrival commits in its OWN transaction so a
        // 300k-tx window ratchets progress arrival by arrival; and a CRDB 40001
        // abort poisons the surrounding transaction (25P02 on any further
        // statement), so a retry needs a fresh transaction per attempt.
        this.arrivalTx = new TransactionTemplate(txManager);
        this.arrivalTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    /**
     * Step-transaction reads are arrival-level scalars ONLY (futured count
     * aggregates + due-arrival listing): the 23x300k whole-backlog per-tx
     * join blew CRDB's sql memory budget (joinreader-mem) live. Every arrival
     * then commits in its own REQUIRES_NEW transaction so one failure never
     * rolls back sibling emissions. A failed arrival is logged and skipped;
     * the window still reports FAILED at the end (the next window re-picks
     * exactly the unclaimed arrivals).
     *
     * @return number of pain.008 files emitted for the run date.
     */
    public int emitDue(final LocalDate runDate) {
        warnFutured(runDate);
        List<DueArrivalRow> arrivals = emissions.findDueArrivals(runDate);
        int emitted = 0;
        int failed = 0;
        for (DueArrivalRow arrival : arrivals) {
            try {
                if (emitArrival(arrival, runDate)) {
                    emitted++;
                }
            } catch (final RuntimeException e) {
                failed++;
                log.error("emission failed stage=CRW arrival={} runDate={}", arrival.arrivalId(), runDate, e);
            }
        }
        if (failed > 0) {
            throw new IllegalStateException("%d of %d due arrivals failed emission for run date %s"
                    .formatted(failed, arrivals.size(), runDate));
        }
        return emitted;
    }

    /**
     * R-38 exclusion visibility without the whole-backlog per-tx fanout:
     * aggregate counts per (arrival, process date) first; per-tx WARN detail
     * only for small groups (uniform historic shape), ONE summary WARN with
     * the count for large ones (300k per-tx lines are log spam AND the
     * memory problem; seq=-1 e2e=- follows the ALREADY_VISIBLE file-level
     * WARN precedent).
     */
    private void warnFutured(final LocalDate runDate) {
        for (FuturedCountRow group : emissions.findFuturedCounts(runDate)) {
            if (group.futured() > FUTURED_DETAIL_WARN_LIMIT) {
                log.warn("excluded stage=CRW arrival={} seq=-1 e2e=- count={} reason=FUTURED_{}",
                        group.arrivalId(), group.futured(), group.processDate());
                continue;
            }
            for (FuturedRow futured : emissions.findFuturedForArrival(group.arrivalId(), group.processDate())) {
                log.warn("excluded stage=CRW arrival={} seq={} e2e={} reason=FUTURED_{}",
                        futured.arrivalId(), futured.sequence(), futured.e2e(), futured.processDate());
            }
        }
    }

    /** One arrival = one committed unit: fresh REQUIRES_NEW tx per bounded-retry attempt. */
    private boolean emitArrival(final DueArrivalRow arrival, final LocalDate runDate) {
        return CrdbRetry.run("emit arrival=%s".formatted(arrival.arrivalId()), () ->
                Boolean.TRUE.equals(arrivalTx.execute(status -> {
                    try {
                        return emitOne(arrival, runDate);
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })));
    }

    private boolean emitOne(final DueArrivalRow arrival, final LocalDate runDate) throws IOException {
        UUID arrivalId = arrival.arrivalId();
        // This arrival's due rows, read INSIDE its own transaction: bounded by
        // one arrival's size (300k max) instead of the whole backlog.
        List<DueRow> due = emissions.findDueForArrival(runDate, arrivalId);
        if (due.isEmpty()) {
            // Every due row failed validation (or drifted away): nothing to
            // emit, no claim taken, a later window re-evaluates this arrival.
            return false;
        }
        String client = arrival.client();
        String msgId = arrival.msgId();
        String fileName = client + "_" + msgId + "_PAIN008.xml";

        // SCRUM-55 interim (Task 1): single ordinal-1 batch, bare source MsgId as outbound
        // identity; the SplitPlanner rewrite (Task 4) replaces this whole method.
        CrwEmissionEntity candidate = CrwEmissionEntity.plannedBatch(null, arrivalId, runDate, 1, msgId, fileName);
        emissions.claimSnapshot(candidate);
        CrwEmissionEntity emission = emissions.findByArrivalIdAndRunDate(arrivalId, runDate).orElseThrow();
        if ("VISIBLE".equals(emission.getState())) {
            // Already handed to Fintegrate by an earlier window of this run date:
            // a later window MUST NOT re-emit (duplicate collection order). Restart
            // rebuilds still happen below while the state is pre-VISIBLE.
            // R-38: single file-level WARN (no per-tx identity at file scope).
            log.warn("excluded stage=CRW arrival={} seq=-1 e2e=- reason=ALREADY_VISIBLE", arrivalId);
            return false;
        }
        if ("PLANNED".equals(emission.getState())) {
            for (DueRow row : due) {
                members.addMember(CrwEmissionMemberEntity.of(emission.getId(), row.sequence(),
                        row.e2e(), row.amount()));
            }
            emissions.transition(emission.getId(), "MATERIALIZED");
        }
        // Build strictly from the immutable snapshot, never the live selection (R-24).
        List<CrwEmissionMemberEntity> snapshot = members.findByEmissionIdOrderBySequence(emission.getId());
        BigDecimal controlSum = snapshot.stream().map(CrwEmissionMemberEntity::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        List<String> xml = painWriter.build(msgId, snapshot, controlSum);
        // SCRUM-42: per-client fint-req/out leaf. An unconfigured client fails closed here (resolve throws).
        StagedWrite.write(layout.resolve(client, ExchangeChannel.FINT_REQ, ExchangeSub.OUT).resolve(fileName), xml);
        emissions.transition(emission.getId(), "VISIBLE");
        return true;
    }
}
