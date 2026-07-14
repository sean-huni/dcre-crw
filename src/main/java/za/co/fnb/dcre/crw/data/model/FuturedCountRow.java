package za.co.fnb.dcre.crw.data.model;

import java.time.LocalDate;
import java.util.UUID;

/** Futured (warehoused) transaction count per (arrival, process date) group (R-38 at scale). */
public record FuturedCountRow(UUID arrivalId, LocalDate processDate, long futured) {
}
