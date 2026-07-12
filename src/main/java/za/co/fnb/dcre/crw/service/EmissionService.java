package za.co.fnb.dcre.crw.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.CrwEmissionMemberEntity;
import za.co.fnb.dcre.crw.data.model.DueRow;
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
        Map<UUID, List<DueRow>> byArrival = new LinkedHashMap<>();
        for (DueRow row : emissions.findDue(runDate)) {
            byArrival.computeIfAbsent(row.arrivalId(), k -> new java.util.ArrayList<>()).add(row);
        }
        int emitted = 0;
        for (var entry : byArrival.entrySet()) {
            emitOne(entry.getKey(), runDate, entry.getValue());
            emitted++;
        }
        return emitted;
    }

    private void emitOne(UUID arrivalId, LocalDate runDate, List<DueRow> due) throws IOException {
        String client = due.get(0).client();
        String msgId = due.get(0).msgId();
        String fileName = client + "_" + msgId + "_PAIN008.xml";

        CrwEmissionEntity candidate = CrwEmissionEntity.planned(arrivalId, runDate, fileName);
        emissions.claimSnapshot(candidate);
        CrwEmissionEntity emission = emissions.findByArrivalIdAndRunDate(arrivalId, runDate).orElseThrow();
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
    }
}
