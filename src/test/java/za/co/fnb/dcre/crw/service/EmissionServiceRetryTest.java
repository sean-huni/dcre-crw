package za.co.fnb.dcre.crw.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
import org.springframework.dao.CannotAcquireLockException;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.CrwEmissionMemberEntity;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
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
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * SCRUM-42 load fix at SCRUM-55 batch grain: per-arrival REQUIRES_NEW
 * transactions in emitDue. A CRDB 40001 abort on one parent is retried in a
 * fresh transaction (PRG CrdbRetry shape); a persistently failing parent is
 * logged, skipped, and reported at the end WITHOUT aborting the loop, so
 * survivor parents still emit.
 */
class EmissionServiceRetryTest {

    private static final LocalDate RUN_DATE = LocalDate.parse("2026-07-14");
    private static final UUID GOOD_ARRIVAL = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private static final UUID BAD_ARRIVAL = UUID.fromString("22222222-2222-4222-8222-222222222222");

    private static final CannotAcquireLockException ABORT = new CannotAcquireLockException(
            "PreparedStatementCallback; ERROR: restart transaction: TransactionRetryWithProtoRefreshError:"
                    + " WriteTooOldError");

    @TempDir
    Path root;

    private CrwEmissionRepo emissions;
    private CrwEmissionMemberRepo members;
    private SplitPlanner planner;
    private EmissionService service;

    @BeforeEach
    void setUp() {
        emissions = mock(CrwEmissionRepo.class);
        members = mock(CrwEmissionMemberRepo.class);
        planner = mock(SplitPlanner.class);
        final ExchangeLayout layout = new ExchangeLayout(root, Map.of("FNBRF01",
                Map.of(ExchangeChannel.FINT_REQ, Map.of(ExchangeSub.OUT, "fnbrf01/fint-req/out"))));
        service = new EmissionService(emissions, members, new Pain008Writer(), layout, planner,
                new ResourcelessTransactionManager());
        when(emissions.findFuturedCounts(RUN_DATE)).thenReturn(List.of());
    }

    @Test
    void transientAbortOnOneArrivalRetriesInFreshTransactionThenEmits() {
        when(emissions.findDueArrivals(RUN_DATE)).thenReturn(List.of(
                new DueArrivalRow(GOOD_ARRIVAL, "FNBRF01", "MSGA")));
        final CrwEmissionEntity batch = stubBatch("MSGA", "FNBRF01_MSGA_PAIN008.xml");
        when(planner.planAndClaim(any(), eq(RUN_DATE)))
                .thenThrow(ABORT).thenThrow(ABORT).thenReturn(List.of(batch));
        stubMembers(batch);

        final int emitted = service.emitDue(RUN_DATE);

        assertEquals(1, emitted, "two transient aborts on the parent must retry then succeed");
        verify(planner, times(3)).planAndClaim(any(), eq(RUN_DATE));
        assertTrue(Files.exists(root.resolve("fnbrf01/fint-req/out/FNBRF01_MSGA_PAIN008.xml")),
                "the retried parent's pain.008 lands in the per-client leaf");
    }

    @Test
    void failedArrivalIsSkippedSurvivorsEmitAndTheWindowReportsFailure() {
        // The failing parent comes FIRST: the loop must carry on past it.
        when(emissions.findDueArrivals(RUN_DATE)).thenReturn(List.of(
                new DueArrivalRow(BAD_ARRIVAL, "FNBXX99", "MSGB"),
                new DueArrivalRow(GOOD_ARRIVAL, "FNBRF01", "MSGA")));
        final CrwEmissionEntity badBatch = stubBatch("MSGB", "FNBXX99_MSGB_PAIN008.xml");
        final CrwEmissionEntity goodBatch = stubBatch("MSGA", "FNBRF01_MSGA_PAIN008.xml");
        when(planner.planAndClaim(argThat(a -> a != null && BAD_ARRIVAL.equals(a.arrivalId())), eq(RUN_DATE)))
                .thenReturn(List.of(badBatch));
        when(planner.planAndClaim(argThat(a -> a != null && GOOD_ARRIVAL.equals(a.arrivalId())), eq(RUN_DATE)))
                .thenReturn(List.of(goodBatch));
        stubMembers(badBatch);
        stubMembers(goodBatch);

        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> service.emitDue(RUN_DATE),
                "a failed parent keeps the window outcome honest: emitDue reports FAILED");

        assertTrue(failure.getMessage().contains("1 of 2"),
                "summary names the failed/total arrival counts: " + failure.getMessage());
        assertTrue(Files.exists(root.resolve("fnbrf01/fint-req/out/FNBRF01_MSGA_PAIN008.xml")),
                "the survivor parent emitted despite the earlier parent's failure");
    }

    /** Built batch stub: MATERIALIZED_MEMBERS with frozen totals matching the one stubbed member. */
    private CrwEmissionEntity stubBatch(final String outboundMsgId, final String fileName) {
        final CrwEmissionEntity batch = mock(CrwEmissionEntity.class);
        when(batch.getId()).thenReturn(UUID.randomUUID());
        when(batch.getState()).thenReturn("MATERIALIZED_MEMBERS");
        when(batch.getBatchOrdinal()).thenReturn(1);
        when(batch.getOutboundMsgId()).thenReturn(outboundMsgId);
        when(batch.getFileName()).thenReturn(fileName);
        when(batch.getTxCount()).thenReturn(1L);
        when(batch.getControlSum()).thenReturn(new BigDecimal("10.00"));
        return batch;
    }

    private void stubMembers(final CrwEmissionEntity batch) {
        // read the mocked id into a local FIRST: a mock call inside thenReturn's
        // argument list leaves the stubbing unfinished (Mockito hint 3)
        final UUID id = batch.getId();
        when(members.findByEmissionIdOrderBySequence(id)).thenReturn(List.of(
                CrwEmissionMemberEntity.of(id, 1, "E2E1", new BigDecimal("10.00"))));
    }
}
