package za.co.fnb.dcre.crw.data.model;

import org.springframework.data.relational.core.mapping.Table;
import za.co.fnb.dcre.platform.persistence.BaseEntity;

import java.math.BigDecimal;
import java.util.UUID;

@Table("crw_emission_member")
public class CrwEmissionMemberEntity extends BaseEntity {

    private UUID emissionId;
    private Integer sequence;
    private String e2e;
    private BigDecimal amount;

    public static CrwEmissionMemberEntity of(UUID emissionId, int sequence, String e2e, BigDecimal amount) {
        CrwEmissionMemberEntity e = new CrwEmissionMemberEntity();
        e.emissionId = emissionId;
        e.sequence = sequence;
        e.e2e = e2e;
        e.amount = amount;
        return e;
    }

    public UUID getEmissionId() { return emissionId; }
    public Integer getSequence() { return sequence; }
    public String getE2e() { return e2e; }
    public BigDecimal getAmount() { return amount; }
}
