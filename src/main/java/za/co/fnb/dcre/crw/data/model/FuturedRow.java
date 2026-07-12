package za.co.fnb.dcre.crw.data.model;

import java.time.LocalDate;
import java.util.UUID;

/** Read projection of a scheduled-but-not-due transaction (R-38 exclusion visibility). */
public record FuturedRow(UUID arrivalId, Integer sequence, String e2e, LocalDate processDate) {
}
