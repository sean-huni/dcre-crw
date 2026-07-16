package za.co.fnb.dcre.crw.data.model;

import org.springframework.data.relational.core.mapping.Table;
import za.co.fnb.dcre.platform.persistence.BaseEntity;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

/**
 * SCRUM-55: one row per (arrival, run date) parent, the durable split plan.
 * The applied max and batch count are FROZEN here at plan time: a config
 * change never repartitions an existing plan (spec section 3).
 */
@Table("crw_emission_group")
public class CrwEmissionGroupEntity extends BaseEntity {

    private UUID arrivalId;
    private String client;
    private String sourceMsgId;
    private LocalDate runDate;
    private int appliedMax;
    private long totalTx;
    private BigDecimal totalAmount;
    private int expectedBatchCount;
    private boolean split;

    public static CrwEmissionGroupEntity planned(final UUID arrivalId, final String client,
            final String sourceMsgId, final LocalDate runDate, final int appliedMax, final long totalTx,
            final BigDecimal totalAmount, final int expectedBatchCount, final boolean split) {
        CrwEmissionGroupEntity g = new CrwEmissionGroupEntity();
        g.assignIdIfMissing();
        g.arrivalId = arrivalId;
        g.client = client;
        g.sourceMsgId = sourceMsgId;
        g.runDate = runDate;
        g.appliedMax = appliedMax;
        g.totalTx = totalTx;
        g.totalAmount = totalAmount;
        g.expectedBatchCount = expectedBatchCount;
        g.split = split;
        return g;
    }

    public UUID getArrivalId() { return arrivalId; }
    public String getClient() { return client; }
    public String getSourceMsgId() { return sourceMsgId; }
    public LocalDate getRunDate() { return runDate; }
    public int getAppliedMax() { return appliedMax; }
    public long getTotalTx() { return totalTx; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public int getExpectedBatchCount() { return expectedBatchCount; }
    public boolean isSplit() { return split; }
}
