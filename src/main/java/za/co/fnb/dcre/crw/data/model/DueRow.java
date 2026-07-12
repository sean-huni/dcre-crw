package za.co.fnb.dcre.crw.data.model;

import java.math.BigDecimal;
import java.util.UUID;

/** Read projection of a transaction due for emission (joins CRR/CTV/CDE data). */
public record DueRow(UUID arrivalId, String client, String msgId, Integer sequence,
                     String e2e, BigDecimal amount) {
}
