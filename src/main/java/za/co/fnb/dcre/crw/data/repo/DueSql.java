package za.co.fnb.dcre.crw.data.repo;

/**
 * The due-path SQL, split into per-arm fragments (A-78).
 *
 * <p>These are the SAME predicates the queries carried as one UNION before; they are separated
 * only so an arm whose tables have not bootstrapped can be left out of the composed statement.
 * When both arms are resolvable the composed SQL is identical to the original, UNION included,
 * so the emission semantics are unchanged.
 */
final class DueSql {

    /**
     * SCRUM-69 pay-arm gates, SINGLE SOURCE for every pay-eligibility arm (review B1/M1).
     * A flow='PAY' parent is due when:
     * - M1: the AIS verdict set COVERS the PASS set (count comparison, not bare EXISTS): a
     *   slice-committing mid-run AIS or an AIS that died stays fail-closed until the last
     *   verdict lands;
     * - at least one PASS row exists (nothing validated = nothing to emit);
     * - the run date is on/after the ingest day (pay rows are immediate);
     * - B1: NO emission exists for a STRICTLY EARLIER run date. Strictly earlier keeps the
     *   same-day crash-resume visible (day-1 rows never exclude a day-1 re-poll) while a fully
     *   or partially emitted day-1 parent is never due again on day 2 (recovery re-runs day 1).
     */
    static final String PAY_DUE_GATES = """
            h.flow = 'PAY'
              AND (SELECT count(*) FROM ais_verdict av WHERE av.arrival_id = h.arrival_id)
                  >= (SELECT count(*) FROM validation_log vp WHERE vp.arrival_id = h.arrival_id
                      AND vp.outcome = 'PASS')
              AND EXISTS (SELECT 1 FROM validation_log vp WHERE vp.arrival_id = h.arrival_id
                          AND vp.outcome = 'PASS')
              AND :runDate >= CAST(h.created_at AS DATE)
              AND NOT EXISTS (SELECT 1 FROM crw_emission e
                              WHERE e.arrival_id = h.arrival_id AND e.run_date < :runDate)""";

    /** Pay-flow member rows: the parent's PASS rows joined to the spine, no cde reference. */
    static final String PAY_MEMBER_ROWS = """
            FROM tx_header h
                JOIN validation_log v ON v.arrival_id = h.arrival_id AND v.outcome = 'PASS'
                JOIN tx_entry t ON t.arrival_id = h.arrival_id AND t.sequence = v.sequence
                WHERE h.arrival_id = :arrivalId AND
            """ + PAY_DUE_GATES;

    /** DC member rows: the parent's PASS rows scheduled for the run date. */
    static final String DC_MEMBER_ROWS = """
            FROM cde_schedule s
                JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence
                    AND v.outcome = 'PASS'
                JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
                WHERE s.process_date = :runDate AND s.arrival_id = :arrivalId""";

    static final String DC_ARRIVALS = """
            SELECT DISTINCT s.arrival_id, h.initg_pty, h.msg_id
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate""";

    static final String PAY_ARRIVALS = """
            SELECT h.arrival_id, h.initg_pty, h.msg_id
            FROM tx_header h
            WHERE
            """ + PAY_DUE_GATES;

    static final String DC_ARRIVALS_FOR_CLIENT = """
            SELECT DISTINCT s.arrival_id, h.initg_pty, h.msg_id, h.created_at
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate AND h.initg_pty = :client""";

    static final String PAY_ARRIVALS_FOR_CLIENT = """
            SELECT h.arrival_id, h.initg_pty, h.msg_id, h.created_at
            FROM tx_header h
            WHERE h.initg_pty = :client AND
            """ + PAY_DUE_GATES;

    static final String DC_CLIENTS = """
            SELECT DISTINCT h.initg_pty
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate""";

    static final String PAY_CLIENTS = """
            SELECT h.initg_pty
            FROM tx_header h
            WHERE
            """ + PAY_DUE_GATES;

    /**
     * Warehoused (futured) counts per (arrival, process date) past the run date (R-38 at scale):
     * pure aggregate, never the per-tx fanout that exceeded the sql memory budget. DC only, since
     * pay rows are never warehoused.
     */
    static final String FUTURED_COUNTS = """
            SELECT s.arrival_id, s.process_date, count(*) AS futured
            FROM cde_schedule s
            JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence
                AND v.outcome = 'PASS'
            WHERE s.process_date > :runDate
            GROUP BY s.arrival_id, s.process_date
            ORDER BY s.arrival_id, s.process_date""";

    /** Client-scoped variant, so parallel lanes never duplicate an R-38 WARN. */
    static final String FUTURED_COUNTS_FOR_CLIENT = """
            SELECT s.arrival_id, s.process_date, count(*) AS futured
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence
                AND v.outcome = 'PASS'
            WHERE s.process_date > :runDate AND h.initg_pty = :client
            GROUP BY s.arrival_id, s.process_date
            ORDER BY s.arrival_id, s.process_date""";

    private DueSql() {
    }
}
