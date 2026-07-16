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

    /** Delegates to the real writer; while failing, the SECOND build (batch 2) throws, after file 1 landed. */
    static final class FlakyPain008Writer extends Pain008Writer {

        private final AtomicInteger builds = new AtomicInteger();
        private volatile boolean failing;

        void failAfterFirstBuild() {
            failing = true;
            builds.set(0);
        }

        void heal() {
            failing = false;
        }

        @Override
        public List<String> build(final String outboundMsgId, final List<CrwEmissionMemberEntity> snapshot,
                                  final BigDecimal controlSum) {
            if (failing && builds.incrementAndGet() > 1) {
                throw new IllegalStateException("simulated crash between batch 1 VISIBLE and batch 2");
            }
            return super.build(outboundMsgId, snapshot, controlSum);
        }
    }
}
