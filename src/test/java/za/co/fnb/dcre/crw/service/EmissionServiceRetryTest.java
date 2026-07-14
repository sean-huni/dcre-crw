package za.co.fnb.dcre.crw.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
import org.springframework.dao.CannotAcquireLockException;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.CrwEmissionMemberEntity;
import za.co.fnb.dcre.crw.data.model.DueRow;
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
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * SCRUM-42 load fix: per-arrival REQUIRES_NEW transactions in emitDue. A CRDB
 * 40001 abort on one arrival is retried in a fresh transaction (PRG CrdbRetry
 * shape); a persistently failing arrival is logged, skipped, and reported at
 * the end WITHOUT aborting the loop, so survivor arrivals still emit.
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
    private EmissionService service;

    @BeforeEach
    void setUp() {
        emissions = mock(CrwEmissionRepo.class);
        members = mock(CrwEmissionMemberRepo.class);
        final ExchangeLayout layout = new ExchangeLayout(root, Map.of("FNBRF01",
                Map.of(ExchangeChannel.FINT_REQ, Map.of(ExchangeSub.OUT, "fnbrf01/fint-req/out"))));
        service = new EmissionService(emissions, members, new Pain008Writer(), layout,
                new ResourcelessTransactionManager());
        when(emissions.findFutured(RUN_DATE)).thenReturn(List.of());
    }

    @Test
    void transientAbortOnOneArrivalRetriesInFreshTransactionThenEmits() throws Exception {
        when(emissions.findDue(RUN_DATE)).thenReturn(List.of(due(GOOD_ARRIVAL, "FNBRF01", "MSGA")));
        final CrwEmissionEntity emission = stubEmission(GOOD_ARRIVAL, "FNBRF01_MSGA_PAIN008.xml");
        doThrow(ABORT).doThrow(ABORT).doNothing().when(emissions).claimSnapshot(any());
        when(members.findByEmissionIdOrderBySequence(emission.getId())).thenReturn(List.of(
                CrwEmissionMemberEntity.of(emission.getId(), 1, "E2EMSGA1", new BigDecimal("10.00"))));

        final int emitted = service.emitDue(RUN_DATE);

        assertEquals(1, emitted, "two transient aborts on the arrival must retry then succeed");
        verify(emissions, times(3)).claimSnapshot(any());
        assertTrue(Files.exists(root.resolve("fnbrf01/fint-req/out/FNBRF01_MSGA_PAIN008.xml")),
                "the retried arrival's pain.008 lands in the per-client leaf");
    }

    @Test
    void failedArrivalIsSkippedSurvivorsEmitAndTheWindowReportsFailure() {
        // The failing arrival comes FIRST: the loop must carry on past it.
        when(emissions.findDue(RUN_DATE)).thenReturn(List.of(
                due(BAD_ARRIVAL, "FNBXX99", "MSGB"),
                due(GOOD_ARRIVAL, "FNBRF01", "MSGA")));
        stubEmission(BAD_ARRIVAL, "FNBXX99_MSGB_PAIN008.xml");
        final CrwEmissionEntity good = stubEmission(GOOD_ARRIVAL, "FNBRF01_MSGA_PAIN008.xml");
        when(members.findByEmissionIdOrderBySequence(good.getId())).thenReturn(List.of(
                CrwEmissionMemberEntity.of(good.getId(), 1, "E2EMSGA1", new BigDecimal("10.00"))));

        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> service.emitDue(RUN_DATE),
                "a failed arrival keeps the window outcome honest: emitDue reports FAILED");

        assertTrue(failure.getMessage().contains("1 of 2"),
                "summary names the failed/total arrival counts: " + failure.getMessage());
        assertTrue(Files.exists(root.resolve("fnbrf01/fint-req/out/FNBRF01_MSGA_PAIN008.xml")),
                "the survivor arrival emitted despite the earlier arrival's failure");
    }

    private DueRow due(final UUID arrival, final String client, final String msgId) {
        return new DueRow(arrival, client, msgId, 1, "E2E" + msgId + "1", new BigDecimal("10.00"));
    }

    private CrwEmissionEntity stubEmission(final UUID arrival, final String fileName) {
        final CrwEmissionEntity emission = CrwEmissionEntity.planned(arrival, RUN_DATE, fileName);
        when(emissions.findByArrivalIdAndRunDate(arrival, RUN_DATE)).thenReturn(Optional.of(emission));
        return emission;
    }
}
