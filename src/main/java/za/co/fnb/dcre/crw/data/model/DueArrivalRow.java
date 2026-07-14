package za.co.fnb.dcre.crw.data.model;

import java.util.UUID;

/**
 * Arrival-level due listing (SCRUM-42 load fix): scalars only, one row per
 * arrival. The per-transaction rows are fetched per arrival inside that
 * arrival's own transaction; the whole-backlog join blew CRDB's sql memory
 * budget (joinreader-mem) at 23 arrivals x 300k transactions.
 */
public record DueArrivalRow(UUID arrivalId, String client, String msgId) {
}
