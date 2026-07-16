package za.co.fnb.dcre.crw.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import za.co.fnb.dcre.crw.CrwTestcontainersBase;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.CrwEmissionMemberEntity;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionMemberRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionRepo;
import za.co.fnb.dcre.platform.files.ExchangeChannel;
import za.co.fnb.dcre.platform.files.ExchangeLayout;
import za.co.fnb.dcre.platform.files.ExchangeSub;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SCRUM-55 Task 4: ordinal batch emission. Publication is strictly ordinal
 * within a parent, restart keys are per batch (arrival, run_date, ordinal)
 * plus the file-existence no-op (R-05), and a VISIBLE batch is never re-sent.
 */
class EmissionSplitIT extends CrwTestcontainersBase {

    @Autowired
    EmissionService service;

    @Autowired
    CrwEmissionRepo emissions;

    @Autowired
    CrwEmissionMemberRepo members;

    @Autowired
    SplitPlanner planner;

    @Autowired
    ExchangeLayout layout;

    @Autowired
    PlatformTransactionManager txManager;

    private Path out() {
        return layout.resolve("FNBRF01", ExchangeChannel.FINT_REQ, ExchangeSub.OUT);
    }

    private long memberTotalAcrossBatches(final UUID arrivalId) {
        return jdbc.queryForObject("SELECT count(*) FROM crw_emission_member m"
                + " JOIN crw_emission e ON e.id = m.emission_id WHERE e.arrival_id = ?", Long.class, arrivalId);
    }

    @Test
    void splitParentEmitsOrdinalFilesInOrder() {
        UUID arrivalId = UUID.randomUUID();
        LocalDate runDate = LocalDate.of(2026, 8, 3);
        seedDueArrival(arrivalId, "FNBRF01", "DCRERF2026071600000010", 12001, runDate);

        service.emitDue(runDate);

        Path out = out();
        assertThat(out.resolve("FNBRF01_DCRERF2026071600000010_1_PAIN008.xml")).exists();
        assertThat(out.resolve("FNBRF01_DCRERF2026071600000010_3_PAIN008.xml")).exists();
        // ordinal publication: visible_at strictly non-decreasing by ordinal
        var batches = emissions.findByArrivalIdAndRunDateOrderByBatchOrdinal(arrivalId, runDate);
        assertThat(batches).extracting(CrwEmissionEntity::getState).containsOnly("VISIBLE");
        assertThat(batches.get(0).getVisibleAt()).isBeforeOrEqualTo(batches.get(1).getVisibleAt());
    }

    @Test
    void killBetweenBatch1VisibleAndBatch2ResumesExactlyTail() throws Exception {
        UUID arrivalId = UUID.randomUUID();
        LocalDate runDate = LocalDate.of(2026, 8, 4);
        seedDueArrival(arrivalId, "FNBRF01", "DCRERF2026071600000011", 10001, runDate);
        // simulate the crash: run once with a writer that throws after the first file lands
        FlakyPain008Writer flaky = new FlakyPain008Writer();
        EmissionService flakyService = new EmissionService(emissions, members, flaky, layout, planner, txManager);
        flaky.failAfterFirstBuild();

        assertThatThrownBy(() -> flakyService.emitDue(runDate)).isInstanceOf(IllegalStateException.class);

        Path b1 = out().resolve("FNBRF01_DCRERF2026071600000011_1_PAIN008.xml");
        assertThat(b1).exists();
        byte[] before = Files.readAllBytes(b1);
        flaky.heal();
        flakyService.emitDue(runDate); // resume
        assertThat(Files.readAllBytes(b1)).isEqualTo(before);             // batch 1 untouched
        assertThat(out().resolve("FNBRF01_DCRERF2026071600000011_2_PAIN008.xml")).exists();
        assertThat(memberTotalAcrossBatches(arrivalId)).isEqualTo(10001L); // no dup members
    }

    /**
     * Cross-entity collision regression (SCRUM-55 review BLOCKER): the same
     * arrival becomes due AGAIN when its warehoused rows mature on a later
     * run date. Day 2 is a DISTINCT entity under the full identity
     * (arrival_id, run_date, batch_ordinal), so it must emit a NEW outbound
     * artifact (sequence continues per source MsgId), never reuse the bare
     * MsgId (a reuse is either a DuplicateKeyException on the outbound
     * unique index, a StagedWrite restart no-op that silently drops the
     * matured money, or a duplicate MsgId on the wire).
     */
    @Test
    void futuredRowsMaturingOnALaterRunDateEmitANewOutboundArtifact() throws Exception {
        UUID arrivalId = UUID.randomUUID();
        LocalDate dayOne = LocalDate.of(2026, 8, 6);
        LocalDate dayTwo = LocalDate.of(2026, 8, 7);
        seedTwoDateArrival(arrivalId, "FNBRF01", "DCRERF2026071600000013", 5, 4, dayOne, dayTwo);

        assertThat(service.emitDue(dayOne)).isEqualTo(1);
        Path d1 = out().resolve("FNBRF01_DCRERF2026071600000013_PAIN008.xml");
        assertThat(d1).exists();
        byte[] dayOneBytes = Files.readAllBytes(d1);

        assertThat(service.emitDue(dayTwo)).isEqualTo(1);       // day 2: matured rows MUST emit

        Path d2 = out().resolve("FNBRF01_DCRERF2026071600000013_2_PAIN008.xml");
        assertThat(d2).exists();
        assertThat(Files.readAllBytes(d1)).isEqualTo(dayOneBytes); // day-1 artifact untouched
        List<String> xml = Files.readAllLines(d2);
        assertThat(xml).anyMatch(l -> l.contains("<MsgId>DCRERF2026071600000013_2</MsgId>"));
        assertThat(xml).anyMatch(l -> l.contains("<NbOfTxs>4</NbOfTxs>"));

        var dayTwoBatches = emissions.findByArrivalIdAndRunDateOrderByBatchOrdinal(arrivalId, dayTwo);
        assertThat(dayTwoBatches).hasSize(1);
        assertThat(dayTwoBatches.get(0).getOutboundMsgId()).isEqualTo("DCRERF2026071600000013_2");
        assertThat(dayTwoBatches.get(0).getState()).isEqualTo("VISIBLE");
        assertThat(memberTotalAcrossBatches(arrivalId)).isEqualTo(9L); // no dup members across run dates
    }

    /**
     * Durable-effect ordering (SCRUM-55 review fix): the plan transaction
     * must commit plan+members+frozen totals (MATERIALIZED) BEFORE any file
     * is published. A kill between that commit and publication leaves the
     * whole frozen plan durable with ZERO files; the resume publishes
     * exactly the missing batches from the SAME committed rows (no replan
     * drift: identical batch ids, identical names, identical member set).
     * Pre-fix the file write lived inside the arrival transaction, so this
     * crash window instead rolled the plan back under an already-buildable
     * file, orphaning published artifacts.
     */
    @Test
    void killBetweenPlanCommitAndPublicationResumesExactlyTheMissingBatches() throws Exception {
        UUID arrivalId = UUID.randomUUID();
        LocalDate runDate = LocalDate.of(2026, 8, 8);
        seedDueArrival(arrivalId, "FNBRF01", "DCRERF2026071600000014", 12001, runDate);
        FlakyPain008Writer flaky = new FlakyPain008Writer();
        EmissionService flakyService = new EmissionService(emissions, members, flaky, layout, planner, txManager);
        flaky.failEveryBuild(); // crash at the seam: plan committed, no batch published

        assertThatThrownBy(() -> flakyService.emitDue(runDate)).isInstanceOf(IllegalStateException.class);

        // The plan survived the kill: batches durable in MATERIALIZED, members frozen, ZERO files.
        var planned = emissions.findByArrivalIdAndRunDateOrderByBatchOrdinal(arrivalId, runDate);
        assertThat(planned).hasSize(3);
        assertThat(planned).extracting(CrwEmissionEntity::getState).containsOnly("MATERIALIZED");
        assertThat(memberTotalAcrossBatches(arrivalId)).isEqualTo(12001L);
        for (final CrwEmissionEntity batch : planned) {
            assertThat(out().resolve(batch.getFileName())).doesNotExist();
        }
        List<UUID> plannedIds = planned.stream().map(CrwEmissionEntity::getId).toList();

        flaky.heal();
        assertThat(flakyService.emitDue(runDate)).isEqualTo(3); // resume publishes EXACTLY the 3 missing batches

        var resumed = emissions.findByArrivalIdAndRunDateOrderByBatchOrdinal(arrivalId, runDate);
        assertThat(resumed).extracting(CrwEmissionEntity::getId).containsExactlyElementsOf(plannedIds);
        assertThat(resumed).extracting(CrwEmissionEntity::getState).containsOnly("VISIBLE");
        for (final CrwEmissionEntity batch : resumed) {
            assertThat(out().resolve(batch.getFileName())).exists();
        }
        assertThat(memberTotalAcrossBatches(arrivalId)).isEqualTo(12001L); // no member drift
    }

    @Test
    void alreadyVisibleParentIsAPerParentNoOpWarn() {
        UUID arrivalId = UUID.randomUUID();
        LocalDate runDate = LocalDate.of(2026, 8, 5);
        seedDueArrival(arrivalId, "FNBRF01", "DCRERF2026071600000012", 5, runDate);
        assertThat(service.emitDue(runDate)).isEqualTo(1);

        Logger emissionLogger = (Logger) LoggerFactory.getLogger(EmissionService.class);
        ListAppender<ILoggingEvent> warns = new ListAppender<>();
        warns.start();
        emissionLogger.addAppender(warns);
        try {
            // re-run emitDue same day: 0 new files, ALREADY_VISIBLE warn per R-38
            assertThat(service.emitDue(runDate)).isZero();
        } finally {
            emissionLogger.detachAppender(warns);
        }
        assertThat(warns.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage))
                .contains("excluded stage=CRW arrival=" + arrivalId + " seq=-1 e2e=- reason=ALREADY_VISIBLE batch=1");
    }

    /** Delegates to the real writer; while failing, every build past the threshold throws (0 = all). */
    static final class FlakyPain008Writer extends Pain008Writer {

        private final AtomicInteger builds = new AtomicInteger();
        private volatile int failAfterBuilds = Integer.MAX_VALUE;

        /** Crash between batch 1 VISIBLE and batch 2: file 1 lands, the second build throws. */
        void failAfterFirstBuild() {
            failAfterBuilds = 1;
            builds.set(0);
        }

        /** Crash at the plan-commit/publication seam: no batch of the parent ever publishes. */
        void failEveryBuild() {
            failAfterBuilds = 0;
            builds.set(0);
        }

        void heal() {
            failAfterBuilds = Integer.MAX_VALUE;
        }

        @Override
        public List<String> build(final String outboundMsgId, final List<CrwEmissionMemberEntity> snapshot,
                                  final BigDecimal controlSum) {
            if (builds.incrementAndGet() > failAfterBuilds) {
                throw new IllegalStateException("simulated crash during publication (build %d past threshold %d)"
                        .formatted(builds.get(), failAfterBuilds));
            }
            return super.build(outboundMsgId, snapshot, controlSum);
        }
    }
}
