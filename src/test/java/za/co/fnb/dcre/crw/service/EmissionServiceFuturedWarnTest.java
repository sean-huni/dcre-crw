package za.co.fnb.dcre.crw.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
import za.co.fnb.dcre.crw.data.model.FuturedCountRow;
import za.co.fnb.dcre.crw.data.model.FuturedRow;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionMemberRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionRepo;
import za.co.fnb.dcre.platform.files.ExchangeChannel;
import za.co.fnb.dcre.platform.files.ExchangeLayout;
import za.co.fnb.dcre.platform.files.ExchangeSub;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * R-38 exclusion visibility at 300k scale (SCRUM-42 load fix): futured WARNs
 * come from per-(arrival, process date) count aggregates; per-tx detail is
 * fetched only for small groups, a large group collapses to ONE summary WARN
 * (per-tx detail at that scale is log spam AND the joinreader-mem problem).
 */
class EmissionServiceFuturedWarnTest {

    private static final LocalDate RUN_DATE = LocalDate.parse("2026-07-14");
    private static final LocalDate FUTURE_DATE = LocalDate.parse("2026-07-20");
    private static final UUID ARRIVAL = UUID.fromString("33333333-3333-4333-8333-333333333333");

    @TempDir
    Path root;

    private CrwEmissionRepo emissions;
    private EmissionService service;
    private Logger emissionLogger;
    private ListAppender<ILoggingEvent> warns;

    @BeforeEach
    void setUp() {
        emissions = mock(CrwEmissionRepo.class);
        // A-78 removed the block guard these fixtures had to stub out: arm availability is now
        // decided inside the composed SQL, where a mock cannot assert it away. Nothing to state.
        final ExchangeLayout layout = new ExchangeLayout(root, Map.of("FNBRF01",
                Map.of(ExchangeChannel.FINT_REQ, Map.of(ExchangeSub.OUT, "fnbrf01/fint-req/out"))));
        service = new EmissionService(emissions, mock(CrwEmissionMemberRepo.class), new Pain008Writer(),
                layout, mock(SplitPlanner.class), new ResourcelessTransactionManager());
        when(emissions.findDueArrivals(RUN_DATE)).thenReturn(List.of());
        emissionLogger = (Logger) LoggerFactory.getLogger(EmissionService.class);
        warns = new ListAppender<>();
        warns.start();
        emissionLogger.addAppender(warns);
    }

    @AfterEach
    void tearDown() {
        emissionLogger.detachAppender(warns);
    }

    @Test
    void largeFuturedGroupCollapsesToOneSummaryWarnWithoutDetailFetch() {
        when(emissions.findFuturedCounts(RUN_DATE)).thenReturn(List.of(
                new FuturedCountRow(ARRIVAL, FUTURE_DATE, 300000L)));

        service.emitDue(RUN_DATE);

        assertEquals(List.of("excluded stage=CRW arrival=" + ARRIVAL
                        + " seq=-1 e2e=- count=300000 reason=FUTURED_2026-07-20"), excludedWarns(),
                "a 300k-row futured group must produce ONE summary WARN, not 300k lines");
        verify(emissions, never()).findFuturedForArrival(any(), any());
    }

    @Test
    void smallFuturedGroupKeepsPerTransactionWarnDetail() {
        when(emissions.findFuturedCounts(RUN_DATE)).thenReturn(List.of(
                new FuturedCountRow(ARRIVAL, FUTURE_DATE, 2L)));
        when(emissions.findFuturedForArrival(ARRIVAL, FUTURE_DATE)).thenReturn(List.of(
                new FuturedRow(ARRIVAL, 2, "E2EX2", FUTURE_DATE),
                new FuturedRow(ARRIVAL, 4, "E2EX4", FUTURE_DATE)));

        service.emitDue(RUN_DATE);

        assertEquals(List.of(
                        "excluded stage=CRW arrival=" + ARRIVAL + " seq=2 e2e=E2EX2 reason=FUTURED_2026-07-20",
                        "excluded stage=CRW arrival=" + ARRIVAL + " seq=4 e2e=E2EX4 reason=FUTURED_2026-07-20"),
                excludedWarns(), "small groups keep the uniform per-tx R-38 WARN shape");
    }

    private List<String> excludedWarns() {
        return warns.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.contains("excluded stage=CRW"))
                .toList();
    }
}
