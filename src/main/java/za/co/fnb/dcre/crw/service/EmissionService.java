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
 * Each due parent runs TWO durability phases (review fix, durable-effect
 * ordering): the plan transaction commits group, batches, frozen members and
 * totals (MATERIALIZED) BEFORE any file exists; publication then walks the
 * committed batches strictly in ordinal order (_2 never VISIBLE before _1),
 * per batch StagedWrite then markVisible in its own small transaction.
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
    private final TransactionTemplate requiresNewTx;

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
        // statement), so a retry needs a fresh transaction per attempt. The
        // same template serves BOTH the plan transaction and each per-batch
        // publication transaction (SCRUM-55 durable-effect ordering).
        this.requiresNewTx = new TransactionTemplate(txManager);
        this.requiresNewTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    /**
     * Step-transaction reads are arrival-level scalars ONLY (futured count
     * aggregates + due-arrival listing): the 23x300k whole-backlog per-tx
     * join blew CRDB's sql memory budget (joinreader-mem) live. Every arrival
     * then plans in its own REQUIRES_NEW transaction so one failure never
     * rolls back sibling emissions. A failed arrival is logged and skipped;
     * the window still reports FAILED at the end (the next window resumes
     * exactly the unplanned arrivals and unpublished batches). Whole-run
     * variant, kept as the direct entry point for tests and manual runs; the
     * job goes through the client-scoped overload per lane (SCRUM-55
     * Feature 2).
     *
     * @return number of pain.008 FILES emitted for the run date (batch grain).
     */
    /**
     * A-76 (SCRUM-107): the due queries read three tables owned by OTHER services, and none of
     * them exists on a freshly reset database until those services have run. Every CRW window
     * then died with {@code relation "cde_schedule" does not exist} and burned its relaunch
     * budget, when the correct answer is simply that nothing is due. Since R-37 was amended to gate DC
     * DAG_COMPLETE on a CRW emission, that failure also blocked every collections arrival from
     * ever completing.
     *
     * <p>Degrades to a WARN and an empty due-set, per the bootstrap-ordering standard: a
     * dependency that has not bootstrapped yet is not a failure, it is no work. Scoped to this
     * ONE table by name, so a genuine typo or a dropped table elsewhere still fails loudly.
     */
    private boolean dueDependenciesMissing() {
        if (emissions.dueQueryTablesExist()) {
            return false;
        }
        log.warn("the due-query peer tables (cde_schedule, validation_log, tx_header) are not all"
                + " present yet: CDE/CTV/CRR have not run on this database, so nothing is due."
                + " Bootstrap ordering, not a failure.");
        return true;
    }

    public int emitDue(final LocalDate runDate) {
        if (dueDependenciesMissing()) {
            return 0;
        }
        warnFutured(emissions.findFuturedCounts(runDate));
        return emitAll(emissions.findDueArrivals(runDate), runDate);
    }

    /**
     * One client lane's slice of the run (SCRUM-55 Feature 2): FIFO within
     * the client = eligibility order (tx_header insertion order). Futured
     * WARNs are client-scoped so parallel lanes never duplicate them.
     */
    public int emitDue(final LocalDate runDate, final String client) {
        if (dueDependenciesMissing()) {
            return 0;
        }
        warnFutured(emissions.findFuturedCounts(runDate, client));
        return emitAll(emissions.findDueArrivals(runDate, client), runDate);
    }

    /**
     * Lane universe for the partitioner: distinct clients with work due on the run date.
     *
     * <p>A-76: guarded like the two emitDue paths. This is the entry point the PARTITIONED job
     * hits FIRST, so guarding only emitDue left the identical failure reachable and the window
     * still died on a fresh database. Third site of the same shape; the class is now closed.
     */
    public List<String> dueClients(final LocalDate runDate) {
        if (dueDependenciesMissing()) {
            return List.of();
        }
        return emissions.findDueClients(runDate);
    }

    private int emitAll(final List<DueArrivalRow> arrivals, final LocalDate runDate) {
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
    private void warnFutured(final List<FuturedCountRow> groups) {
        for (final FuturedCountRow group : groups) {
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

    /**
     * One parent, two durability phases. Phase 1 (fresh REQUIRES_NEW tx per
     * bounded-retry attempt) commits the WHOLE plan: group, batches, frozen
     * members and totals, MATERIALIZED. NO file leaves phase 1: a file
     * published before its plan commits can be consumed by Fintegrate while
     * a crash rolls the plan back, and a legitimately shifted replan (CDE
     * re-ran) would orphan that file's identity. Phase 2 publishes strictly
     * in ordinal order; after a kill between plan commit and publication the
     * resume publishes exactly the unpublished ordinals of the SAME plan.
     * The batch rows returned by phase 1 carry state AND frozen identity
     * from the one snapshot that also computed the prior-artifact offset
     * (crw-5): publication decides "unpublished" and names files from those
     * rows only, never from a separate committed-rows-at-large read.
     */
    private int emitArrival(final DueArrivalRow arrival, final LocalDate runDate) {
        final List<CrwEmissionEntity> batches = CrdbRetry.run("plan arrival=%s".formatted(arrival.arrivalId()),
                () -> requiresNewTx.execute(status -> planner.planAndClaim(arrival, runDate)));
        if (batches == null || batches.isEmpty()) {
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
            publishBatch(arrival, batch);
            emitted++;
        }
        return emitted;
    }

    /**
     * Publishes ONE committed batch: StagedWrite then markVisible inside a
     * small REQUIRES_NEW transaction per bounded-retry attempt. The XML
     * builds strictly from the immutable member snapshot, never the live
     * selection (R-24), and reconciles against the batch's OWN frozen
     * tx_count; identity (outbound MsgId, file name) comes ONLY from the
     * stored row. A replay is a per-batch file-existence StagedWrite no-op
     * (R-05) and markVisible is state-guarded, so a crash anywhere in this
     * method resumes cleanly.
     */
    private void publishBatch(final DueArrivalRow arrival, final CrwEmissionEntity batch) {
        CrdbRetry.run("publish arrival=%s batch=%d".formatted(arrival.arrivalId(), batch.getBatchOrdinal()),
                () -> requiresNewTx.execute(status -> {
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
                    try {
                        StagedWrite.write(target, xml);      // per-batch file-existence restart no-op (R-05)
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    afterStagedWrite(batch);                 // crash-matrix quadrant-4 seam (production no-op)
                    emissions.markVisible(batch.getId());    // stamps visible_at for the SLA timer
                    return null;
                }));
    }

    /**
     * Crash-matrix quadrant-4 seam: runs after StagedWrite has landed the
     * batch file and before markVisible commits. File writes are not
     * transactional, so a kill in this gap leaves the file durable on disk
     * while the publication transaction rolls back with the row still
     * MATERIALIZED; the resume must hit the R-05 file-existence no-op, never
     * a rewrite. Production no-op, package-private so the crash-matrix IT
     * can inject the kill exactly here.
     */
    void afterStagedWrite(final CrwEmissionEntity batch) {
        // production no-op: crash-matrix test seam (quadrant 4)
    }
}
