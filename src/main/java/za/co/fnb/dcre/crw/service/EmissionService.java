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
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

/**
 * Business tier: the R-37 Process-Date Executor at batch grain (SCRUM-55).
 * Each due parent is planned by the SplitPlanner inside ONE REQUIRES_NEW
 * arrival transaction (snapshot-consistent plan) and its batches are built
 * and published strictly in ordinal order: _2 never VISIBLE before _1.
 * Restart keys are per batch (arrival, run_date, ordinal) plus the
 * file-existence no-op (R-05); each file reconciles against its OWN frozen
 * tx_count before building (R-24 frozen-plan integrity).
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
    private final SplitPlanner planner;
    private final TransactionTemplate arrivalTx;

    public EmissionService(final CrwEmissionRepo emissions, final CrwEmissionMemberRepo members,
                           final Pain008Writer painWriter, final ExchangeLayout layout,
                           final SplitPlanner planner, final PlatformTransactionManager txManager) {
        this.emissions = emissions;
        this.members = members;
        this.painWriter = painWriter;
        this.layout = layout;
        this.planner = planner;
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
     * @return number of pain.008 FILES emitted for the run date (batch grain).
     */
    public int emitDue(final LocalDate runDate) {
        warnFutured(runDate);
        List<DueArrivalRow> arrivals = emissions.findDueArrivals(runDate);
        int emitted = 0;
        int failed = 0;
        for (final DueArrivalRow arrival : arrivals) {
            try {
                emitted += emitArrival(arrival, runDate);
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
        for (final FuturedCountRow group : emissions.findFuturedCounts(runDate)) {
            if (group.futured() > FUTURED_DETAIL_WARN_LIMIT) {
                log.warn("excluded stage=CRW arrival={} seq=-1 e2e=- count={} reason=FUTURED_{}",
                        group.arrivalId(), group.futured(), group.processDate());
                continue;
            }
            for (final FuturedRow futured : emissions.findFuturedForArrival(group.arrivalId(), group.processDate())) {
                log.warn("excluded stage=CRW arrival={} seq={} e2e={} reason=FUTURED_{}",
                        futured.arrivalId(), futured.sequence(), futured.e2e(), futured.processDate());
            }
        }
    }

    /** One parent = one committed unit: fresh REQUIRES_NEW tx per bounded-retry attempt. */
    private int emitArrival(final DueArrivalRow arrival, final LocalDate runDate) {
        Integer files = CrdbRetry.run("emit arrival=%s".formatted(arrival.arrivalId()), () ->
                arrivalTx.execute(status -> {
                    try {
                        return emitParent(arrival, runDate);
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }));
        return files == null ? 0 : files;
    }

    /** Plan + build ALL of the parent's batches inside the one arrival tx; ordinal order = publication order. */
    private int emitParent(final DueArrivalRow arrival, final LocalDate runDate) throws IOException {
        List<CrwEmissionEntity> batches = planner.planAndClaim(arrival, runDate);
        if (batches.isEmpty()) {
            return 0;
        }
        int emitted = 0;
        for (final CrwEmissionEntity batch : batches) {
            if ("VISIBLE".equals(batch.getState())) {
                // Already handed to Fintegrate: a later window MUST NOT re-emit
                // this batch. R-38: single file-level WARN per batch.
                log.warn("excluded stage=CRW arrival={} seq=-1 e2e=- reason=ALREADY_VISIBLE batch={}",
                        arrival.arrivalId(), batch.getBatchOrdinal());
                continue;
            }
            // Build strictly from the immutable snapshot, never the live selection (R-24).
            List<CrwEmissionMemberEntity> snapshot = members.findByEmissionIdOrderBySequence(batch.getId());
            if (batch.getTxCount() == null || snapshot.size() != batch.getTxCount()) {
                throw new IllegalStateException(
                        "frozen-plan mismatch stage=CRW arrival=%s batch=%d members=%d expected=%s"
                                .formatted(arrival.arrivalId(), batch.getBatchOrdinal(), snapshot.size(),
                                        batch.getTxCount()));
            }
            List<String> xml = painWriter.build(batch.getOutboundMsgId(), snapshot, batch.getControlSum());
            // SCRUM-42: per-client fint-req/out leaf. An unconfigured client fails closed here (resolve throws).
            Path target = layout.resolve(arrival.client(), ExchangeChannel.FINT_REQ, ExchangeSub.OUT)
                    .resolve(batch.getFileName());
            StagedWrite.write(target, xml);          // per-batch file-existence restart no-op (R-05)
            emissions.markVisible(batch.getId());    // stamps visible_at for the SLA timer
            emitted++;
        }
        return emitted;
    }
}
