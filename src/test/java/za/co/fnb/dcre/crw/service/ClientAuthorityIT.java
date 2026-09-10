package za.co.fnb.dcre.crw.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import za.co.fnb.dcre.crw.CrwTestcontainersBase;
import za.co.fnb.dcre.crw.data.model.CrwEmissionGroupEntity;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionGroupRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionRepo;
import za.co.fnb.dcre.platform.files.ExchangeChannel;
import za.co.fnb.dcre.platform.files.ExchangeLayout;
import za.co.fnb.dcre.platform.files.ExchangeSub;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A-43, ruled 2026-08-08: {@code tx_header.client_token} is the outbound client authority and
 * {@code crw_emission_group.client} carries it, with {@code initg_pty} as the fallback for an
 * arrival that carries no R-31 filename token.
 *
 * <p>THIS IS THE ONLY CRW TEST THAT CAN SEE THE DIFFERENCE. Every other fixture in the suite
 * seeds one value into both columns, so it passes whether the authority is {@code client_token},
 * {@code initg_pty}, or read from the wrong one entirely. That monoculture is why the derivation
 * stood unexamined for four weeks; the tests below exist to make the next change to it fail loudly
 * rather than quietly.
 *
 * <p>Red-proof (recorded 2026-08-08): reverting {@code DueSql.CLIENT_EXPR} to a bare
 * {@code h.initg_pty} fails {@link #divergentHeaderEmitsUnderTheFilenameTokenNotTheHeader} on the
 * emission-group column, the file name and the directory, and
 * {@link #laneUniverseAndLaneSelectionUseTheSameAuthority} on both the lane universe and the lane
 * selection. All three assertion families were seen red before they were seen green.
 *
 * <p>Note what is NOT asserted, because it is not true: nothing about the emitted pain.008 changes.
 * {@code InitgPty} appears as an element in no message this fleet emits and
 * {@code Pain008Writer.build} takes no client at all, so BankservAfrica receives the same bytes
 * either way. The authority reaches the outside world only through the file NAME and the per-client
 * DIRECTORY, which is exactly what these tests pin.
 */
class ClientAuthorityIT extends CrwTestcontainersBase {

    private static final LocalDate RUN_DATE = LocalDate.of(2026, 8, 20);

    @Autowired
    EmissionService service;

    @Autowired
    CrwEmissionGroupRepo groups;

    /** The due-path queries live on the emission repo as a fragment; this is that fragment. */
    @Autowired
    CrwEmissionRepo dueQueries;

    @Autowired
    ExchangeLayout layout;

    private Path fintReqOut(final String client) {
        return layout.resolve(client, ExchangeChannel.FINT_REQ, ExchangeSub.OUT);
    }

    /**
     * The two homes hold DIFFERENT configured clients, so the emission group column, the file
     * name and the output directory each have somewhere wrong they could land. All three must
     * land on the filename token.
     */
    @Test
    void divergentHeaderEmitsUnderTheFilenameTokenNotTheHeader() {
        final UUID arrivalId = UUID.randomUUID();
        final String msgId = "DCRECC2026082000000101";
        seedDueArrival(arrivalId, "FNBCC02", "FNBRF01", msgId, 3, RUN_DATE);

        service.emitDue(RUN_DATE);

        final CrwEmissionGroupEntity group = groups.findByArrivalIdAndRunDate(arrivalId, RUN_DATE).orElseThrow();
        assertThat(group.getClient())
                .as("crw_emission_group.client carries tx_header.client_token, not initg_pty")
                .isEqualTo("FNBCC02");
        assertThat(fintReqOut("FNBCC02").resolve("FNBCC02_%s_PAIN008.xml".formatted(msgId)))
                .as("outbound file lands in the filename token's own fint-req/out leaf")
                .exists();
        assertThat(fintReqOut("FNBRF01").resolve("FNBRF01_%s_PAIN008.xml".formatted(msgId)))
                .as("nothing is written under the header client's directory")
                .doesNotExist();
        assertThat(fintReqOut("FNBRF01").resolve("FNBCC02_%s_PAIN008.xml".formatted(msgId)))
                .as("the directory and the file name must not come from different sources")
                .doesNotExist();
    }

    /**
     * The lane universe and the per-lane selection are two separate statements over the same
     * fact. Keying one on the token and the other on the header would select a lane by one
     * identity and find nothing due in it, which is a silent empty window rather than a failure.
     */
    @Test
    void laneUniverseAndLaneSelectionUseTheSameAuthority() {
        final UUID arrivalId = UUID.randomUUID();
        final LocalDate runDate = RUN_DATE.plusDays(1);
        seedDueArrival(arrivalId, "FNBCC02", "FNBRF01", "DCRECC2026082000000102", 2, runDate);

        final List<String> lanes = dueQueries.findDueClients(runDate);
        assertThat(lanes).containsExactly("FNBCC02");

        assertThat(dueQueries.findDueArrivals(runDate, "FNBCC02"))
                .as("the lane the universe named must contain the arrival")
                .extracting(DueArrivalRow::arrivalId)
                .containsExactly(arrivalId);
        assertThat(dueQueries.findDueArrivals(runDate, "FNBRF01"))
                .as("the header value must not select a lane")
                .isEmpty();
    }

    /**
     * The NOT NULL hazard, stated as a test. {@code client_token} is nullable by design (a job
     * launched outside AGT has no original filename) and {@code crw_emission_group.client} is
     * NOT NULL, so a bare swap for the authority would convert a silent mis-selection into an
     * insert failure. The fallback is what makes this a correction rather than a new defect.
     */
    @Test
    void absentFilenameTokenFallsBackToTheCopybookHeader() {
        final UUID arrivalId = UUID.randomUUID();
        final LocalDate runDate = RUN_DATE.plusDays(2);
        final String msgId = "DCRECC2026082000000103";
        seedDueArrival(arrivalId, null, "FNBCC01", msgId, 2, runDate);

        service.emitDue(runDate);

        final CrwEmissionGroupEntity group = groups.findByArrivalIdAndRunDate(arrivalId, runDate).orElseThrow();
        assertThat(group.getClient()).isEqualTo("FNBCC01");
        assertThat(fintReqOut("FNBCC01").resolve("FNBCC01_%s_PAIN008.xml".formatted(msgId))).exists();
        assertThat(dueQueries.findDueClients(runDate)).containsExactly("FNBCC01");
    }
}
