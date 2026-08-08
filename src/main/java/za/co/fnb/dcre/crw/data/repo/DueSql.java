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

    /**
     * The outbound client of a {@code tx_header} row: the R-31 filename token when the arrival
     * carried one, else the mandatory copybook {@code destination_id} (A-43, ruled 2026-08-08).
     *
     * <p>{@code client_token} is the R-31 filename token AGT already matched against the
     * drop-zone directory, so it is the "resolved from trusted route/profile config" identity
     * R-16 demands; {@code initg_pty} is an unresolved header value whose value domain is still
     * open as A-19. {@code initg_pty} is the FALLBACK rather than the source because
     * {@code client_token} is nullable by design (a job launched outside AGT carries no original
     * filename) while {@code crw_emission_group.client} is NOT NULL, so a bare swap would turn a
     * silent mis-selection into an insert failure. This is the shape {@code mandates/mrw}
     * ({@code ManRequestHeaderView.client()}) already chose and documented; copying the sibling
     * is the rule.
     *
     * <p>Both columns hold the same 7-character value on every arrival that can reach CRW
     * through AGT, because {@code crr HeaderService} throws {@code FileFatalException} when the
     * filename token differs from the header {@code destination_id}. This is a rename of the
     * authority, not a change of the emitted bytes.
     *
     * <p>THIS IS THE SINGLE POINT at which the outbound client is resolved, deliberately. The
     * value reaches three places from one row: {@code crw_emission_group.client}, the outbound
     * file NAME, and the per-client output DIRECTORY. Applying the authority at the column write
     * instead would move the file name while leaving the directory on the old source, and
     * {@code ExchangeLayout.resolve} cannot object to a directory that is configured, so the two
     * would disagree silently on the first day they differed.
     *
     * <p>ASSEMBLY TRAP, and it fired here on 2026-08-08: a Java text block strips trailing
     * whitespace from every line, so {@code ...:runDate AND """ + CLIENT_EXPR} concatenates to
     * {@code ANDCOALESCE(...)} and the statement is rejected as bad grammar. Every join of a
     * text block to this constant states its separator OUTSIDE the block
     * ({@code """ + " AND " + CLIENT_EXPR}), and {@code DueSqlAssemblyTest} asserts the
     * assembled statements rather than trusting the shape to survive an edit.
     */
    static final String CLIENT_EXPR = "COALESCE(h.client_token, h.initg_pty)";

    /** Member rows: the parent's PASS rows scheduled for the run date. */
    static final String MEMBER_ROWS = """
            FROM cde_schedule s
                JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence
                    AND v.outcome = 'PASS'
                JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
                WHERE s.process_date = :runDate AND s.arrival_id = :arrivalId""";

    static final String ARRIVALS = "SELECT DISTINCT s.arrival_id, " + CLIENT_EXPR + """
             AS client, h.msg_id
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate""";

    static final String ARRIVALS_FOR_CLIENT = "SELECT DISTINCT s.arrival_id, " + CLIENT_EXPR + """
             AS client, h.msg_id, h.created_at
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate"""
            + " AND " + CLIENT_EXPR + " = :client";

    static final String CLIENTS = "SELECT DISTINCT " + CLIENT_EXPR + """
             AS client
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
            WHERE s.process_date > :runDate"""
            + " AND " + CLIENT_EXPR + " = :client\n"
            + """
            GROUP BY s.arrival_id, s.process_date
            ORDER BY s.arrival_id, s.process_date""";

    private DueSql() {
    }
}
