package za.co.fnb.dcre.crw.data.repo;

/**
 * The due-path SQL. ONE lane, so one plain statement per query.
 *
 * <p>CRW emits pain.008 for COLLECTIONS only. The payments lane belongs to PRW, in the payments
 * bounded context, and the arm CRW used to carry for it is gone: it selected on
 * {@code tx_header.flow = 'PAY'} and counted rows in {@code ais_verdict}, a table NO changelog in
 * the estate creates. On a clean v1 database that arm could only ever have dropped out of the
 * UNION, so a CRW window would have reported a successful run having emitted nothing.
 *
 * <p><b>No presence guard, deliberately (A-76, A-78).</b> These statements name
 * {@code cde_schedule}, {@code validation_log}, {@code tx_entry} and {@code tx_header}, all owned
 * by other services, and PostgreSQL resolves every relation a statement names, so a missing one
 * throws. That is now the intended behaviour and nothing here catches it.
 *
 * <p>A-78 tolerated a missing table only because there were TWO arms: leaving one out still left
 * the other running, so a collections-only cluster with no {@code ais_verdict} kept emitting DC.
 * With a single arm that reasoning inverts. Guarding the whole due-set is exactly the A-76 shape
 * this codebase already ruled WORSE than a crash: it would report clean, empty windows forever
 * while the schedule table stayed absent, and since R-37 was amended to gate DAG_COMPLETE on a
 * CRW emission, every collections arrival would sit in DAG_RUNNING with no error anywhere. A
 * crash is loud and burns a relaunch budget until somebody looks; a permanent silent zero is not
 * loud and nobody ever looks. So a missing {@code cde_schedule} fails the window.
 */
final class DueSql {

    /** Member rows: the parent's PASS rows scheduled for the run date. */
    static final String MEMBER_ROWS = """
            FROM cde_schedule s
                JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence
                    AND v.outcome = 'PASS'
                JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
                WHERE s.process_date = :runDate AND s.arrival_id = :arrivalId""";

    static final String ARRIVALS = """
            SELECT DISTINCT s.arrival_id, h.initg_pty, h.msg_id
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate""";

    static final String ARRIVALS_FOR_CLIENT = """
            SELECT DISTINCT s.arrival_id, h.initg_pty, h.msg_id, h.created_at
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate AND h.initg_pty = :client""";

    static final String CLIENTS = """
            SELECT DISTINCT h.initg_pty
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate""";

    /**
     * Warehoused (futured) counts per (arrival, process date) past the run date (R-38 at scale):
     * pure aggregate, never the per-tx fanout that exceeded the sql memory budget.
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
