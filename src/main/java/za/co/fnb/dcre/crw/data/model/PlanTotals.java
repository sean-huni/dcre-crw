package za.co.fnb.dcre.crw.data.model;

import java.math.BigDecimal;

/** Whole-parent totals frozen into the emission group at plan time (SCRUM-55). */
public record PlanTotals(long totalTx, BigDecimal totalAmount) {
}
