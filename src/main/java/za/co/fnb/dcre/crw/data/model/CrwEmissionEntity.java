package za.co.fnb.dcre.crw.data.model;

import org.springframework.data.relational.core.mapping.Table;
import za.co.fnb.dcre.platform.persistence.BaseEntity;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/**
 * SCRUM-55: batch grain. One row per (arrival, run date, batch ordinal);
 * unsplit parents keep exactly one batch with the bare source MsgId as
 * outbound identity. tx_count/control_sum are frozen at plan time (R-24).
 */
@Table("crw_emission")
public class CrwEmissionEntity extends BaseEntity {

    private UUID groupId;
    private UUID arrivalId;
    private LocalDate runDate;
    private int batchOrdinal;
    private String outboundMsgId;
    private String fileName;
    private String state;
    private Long txCount;
    private BigDecimal controlSum;
    private Instant visibleAt;

    public static CrwEmissionEntity plannedBatch(final UUID groupId, final UUID arrivalId, final LocalDate runDate,
            final int batchOrdinal, final String outboundMsgId, final String fileName) {
        CrwEmissionEntity e = new CrwEmissionEntity();
        e.assignIdIfMissing();
        e.groupId = groupId;
        e.arrivalId = arrivalId;
        e.runDate = runDate;
        e.batchOrdinal = batchOrdinal;
        e.outboundMsgId = outboundMsgId;
        e.fileName = fileName;
        e.state = "PLANNED";
        return e;
    }

    public UUID getGroupId() { return groupId; }
    public UUID getArrivalId() { return arrivalId; }
    public LocalDate getRunDate() { return runDate; }
    public int getBatchOrdinal() { return batchOrdinal; }
    public String getOutboundMsgId() { return outboundMsgId; }
    public String getFileName() { return fileName; }
    public String getState() { return state; }
    public Long getTxCount() { return txCount; }
    public BigDecimal getControlSum() { return controlSum; }
    public Instant getVisibleAt() { return visibleAt; }
}
