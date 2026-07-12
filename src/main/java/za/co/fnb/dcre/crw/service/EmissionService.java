package za.co.fnb.dcre.crw.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.CrwEmissionMemberEntity;
import za.co.fnb.dcre.crw.data.model.DueRow;
import za.co.fnb.dcre.crw.data.model.FuturedRow;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionMemberRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionRepo;
import za.co.fnb.dcre.platform.files.StagedWrite;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    private final CrwEmissionRepo emissions;
    private final CrwEmissionMemberRepo members;
    private final Pain008Writer painWriter;
    private final String exchangeRoot;

    public EmissionService(CrwEmissionRepo emissions, CrwEmissionMemberRepo members,
                           Pain008Writer painWriter,
                           @Value("${dcre.exchange-root}") String exchangeRoot) {
        this.emissions = emissions;
        this.members = members;
        this.painWriter = painWriter;
        this.exchangeRoot = exchangeRoot;
    }

    /** @return number of pain.008 files emitted for the run date. */
    public int emitDue(LocalDate runDate) throws IOException {
        // R-38 exclusion visibility: one WARN per futured (warehoused) transaction.
        for (FuturedRow futured : emissions.findFutured(runDate)) {
            log.warn("excluded stage=CRW arrival={} seq={} e2e={} reason=FUTURED_{}",
                    futured.arrivalId(), futured.sequence(), futured.e2e(), futured.processDate());
        }
        Map<UUID, List<DueRow>> byArrival = new LinkedHashMap<>();
        for (DueRow row : emissions.findDue(runDate)) {
            byArrival.computeIfAbsent(row.arrivalId(), k -> new java.util.ArrayList<>()).add(row);
        }
        int emitted = 0;
        for (var entry : byArrival.entrySet()) {
            if (emitOne(entry.getKey(), runDate, entry.getValue())) {
                emitted++;
            }
        }
        return emitted;
    }

    private boolean emitOne(UUID arrivalId, LocalDate runDate, List<DueRow> due) throws IOException {
        String client = due.getFirst().client();
        String msgId = due.getFirst().msgId();
        String fileName = client + "_" + msgId + "_PAIN008.xml";

        CrwEmissionEntity candidate = CrwEmissionEntity.planned(arrivalId, runDate, fileName);
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
        StagedWrite.write(Path.of(exchangeRoot, "fint-req", fileName), xml);
        emissions.transition(emission.getId(), "VISIBLE");
        return true;
    }
}
