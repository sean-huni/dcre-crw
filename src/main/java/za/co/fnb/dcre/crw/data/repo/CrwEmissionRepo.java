package za.co.fnb.dcre.crw.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.model.FuturedCountRow;
import za.co.fnb.dcre.crw.data.model.FuturedRow;
import za.co.fnb.dcre.crw.data.model.PlanTotals;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

public interface CrwEmissionRepo extends CrudRepository<CrwEmissionEntity, UUID> {

    /**
     * SCRUM-69 pay-arm gates, SINGLE SOURCE for every pay-eligibility arm
     * (review B1/M1). A flow='PAY' parent is due when:
     * - M1: the AIS verdict set COVERS the PASS set (count comparison, not
     *   bare EXISTS): a slice-committing mid-run AIS or an AIS that died
     *   stays fail-closed until the last verdict lands;
     * - at least one PASS row exists (nothing validated = nothing to emit);
     * - the run date is on/after the ingest day (pay rows are immediate);
     * - B1: NO emission exists for a STRICTLY EARLIER run date. Strictly
     *   earlier keeps the same-day crash-resume visible (day-1 rows never
     *   exclude a day-1 re-poll) while a fully or partially emitted day-1
     *   parent is never due again on day 2 (recovery re-runs day 1).
     */
    String PAY_DUE_GATES = """
            h.flow = 'PAY'
              AND (SELECT count(*) FROM ais_verdict av WHERE av.arrival_id = h.arrival_id)
                  >= (SELECT count(*) FROM validation_log vp WHERE vp.arrival_id = h.arrival_id
                      AND vp.outcome = 'PASS')
              AND EXISTS (SELECT 1 FROM validation_log vp WHERE vp.arrival_id = h.arrival_id
                          AND vp.outcome = 'PASS')
              AND :runDate >= CAST(h.created_at AS DATE)
              AND NOT EXISTS (SELECT 1 FROM crw_emission e
                              WHERE e.arrival_id = h.arrival_id AND e.run_date < :runDate)""";

    /**
     * Pay-flow member rows (SCRUM-69): the parent's PASS rows joined to the
     * spine, no cde reference; guarded by the same single-source gates.
     */
    String PAY_MEMBER_ROWS = """
            FROM tx_header h
                JOIN validation_log v ON v.arrival_id = h.arrival_id AND v.outcome = 'PASS'
                JOIN tx_entry t ON t.arrival_id = h.arrival_id AND t.sequence = v.sequence
                WHERE h.arrival_id = :arrivalId AND
            """ + PAY_DUE_GATES;

    /**
     * Arrivals with at least one transaction scheduled for the run date
     * (SCRUM-42 load fix): scalars only, one row per arrival, no per-tx
     * fanout. The 23x300k whole-backlog join blew CRDB's sql memory budget
     * (joinreader-mem) live, so per-tx work happens per arrival inside its
     * own transaction (SplitPlanner planning queries + set-based member
     * claims). Validation is checked there too: an arrival whose due rows
     * all failed validation lists here, plans empty, and is skipped without
     * a claim (same net outcome as the old joined query).
     * SCRUM-69 pay arm: flow='PAY' rows have NO cde_schedule; they are due
     * from ingest day once AIS has verdicted and validation passed.
     */
    @Query(value = """
            SELECT DISTINCT s.arrival_id, h.initg_pty, h.msg_id
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate
            UNION
            SELECT h.arrival_id, h.initg_pty, h.msg_id
            FROM tx_header h
            WHERE
            """ + PAY_DUE_GATES + "\nORDER BY arrival_id",
            rowMapperClass = DueArrivalRowMapper.class)
    List<DueArrivalRow> findDueArrivals(@Param("runDate") LocalDate runDate);

    /**
     * One client lane's due parents (SCRUM-55 Feature 2), FIFO by eligibility
     * order: tx_header insertion order (created_at), arrival id as the
     * deterministic tie-break (insertion order, Sean ruling 9). The ORDER BY
     * spans BOTH arms of the union, so DC and pay parents interleave in one
     * FIFO sequence (SCRUM-69).
     */
    @Query(value = """
            SELECT DISTINCT s.arrival_id, h.initg_pty, h.msg_id, h.created_at
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate AND h.initg_pty = :client
            UNION
            SELECT h.arrival_id, h.initg_pty, h.msg_id, h.created_at
            FROM tx_header h
            WHERE h.initg_pty = :client AND
            """ + PAY_DUE_GATES + "\nORDER BY created_at, arrival_id",
            rowMapperClass = DueArrivalRowMapper.class)
    List<DueArrivalRow> findDueArrivals(@Param("runDate") LocalDate runDate, @Param("client") String client);

    /** The lane universe for the partitioner: distinct clients with work due on the run date (both flows, SCRUM-69). */
    @Query(value = """
            SELECT DISTINCT h.initg_pty
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate
            UNION
            SELECT h.initg_pty
            FROM tx_header h
            WHERE
            """ + PAY_DUE_GATES + "\nORDER BY initg_pty",
            rowMapperClass = ClientRowMapper.class)
    List<String> findDueClients(@Param("runDate") LocalDate runDate);

    /**
     * Warehoused (futured) counts per (arrival, process date) group past the
     * run date (R-38 at scale): pure aggregate, never the per-tx fanout that
     * exceeded the sql memory budget on the whole backlog.
     */
    @Query(value = """
            SELECT s.arrival_id, s.process_date, count(*) AS futured
            FROM cde_schedule s
            JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence AND v.outcome = 'PASS'
            WHERE s.process_date > :runDate
            GROUP BY s.arrival_id, s.process_date
            ORDER BY s.arrival_id, s.process_date""", rowMapperClass = FuturedCountRowMapper.class)
    List<FuturedCountRow> findFuturedCounts(@Param("runDate") LocalDate runDate);

    /**
     * Client-scoped futured counts for a lane run (SCRUM-55 Feature 2): each
     * lane warns only its own client's warehoused rows so parallel lanes never
     * duplicate an R-38 WARN.
     */
    @Query(value = """
            SELECT s.arrival_id, s.process_date, count(*) AS futured
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence AND v.outcome = 'PASS'
            WHERE s.process_date > :runDate AND h.initg_pty = :client
            GROUP BY s.arrival_id, s.process_date
            ORDER BY s.arrival_id, s.process_date""", rowMapperClass = FuturedCountRowMapper.class)
    List<FuturedCountRow> findFuturedCounts(@Param("runDate") LocalDate runDate, @Param("client") String client);

    /** ONE (arrival, process date) group's warehoused rows, for per-tx R-38 WARN detail on small groups. */
    @Query(value = """
            SELECT t.arrival_id, t.sequence, t.e2e, s.process_date
            FROM cde_schedule s
            JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence AND v.outcome = 'PASS'
            JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
            WHERE s.arrival_id = :arrivalId AND s.process_date = :processDate
            ORDER BY t.sequence""", rowMapperClass = FuturedRowMapper.class)
    List<FuturedRow> findFuturedForArrival(@Param("arrivalId") UUID arrivalId,
                                           @Param("processDate") LocalDate processDate);

    /**
     * Whole-parent totals for the frozen plan (SCRUM-55): executes inside the
     * caller's arrival transaction, so totals, boundaries and member claims
     * share one CRDB snapshot and the due-set cannot shift mid-plan. The pay
     * arm (SCRUM-69) is the single-source PAY_MEMBER_ROWS fragment; UNION on
     * (sequence, amount) keeps a row single-counted even if both arms ever
     * matched.
     */
    @Query(value = """
            SELECT count(*) AS total_tx, COALESCE(sum(m.amount), 0) AS total_amount FROM (
                SELECT t.sequence, t.amount
                FROM cde_schedule s
                JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence AND v.outcome = 'PASS'
                JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
                WHERE s.process_date = :runDate AND s.arrival_id = :arrivalId
                UNION
                SELECT t.sequence, t.amount
                """ + PAY_MEMBER_ROWS + ") AS m",
            rowMapperClass = PlanTotalsRowMapper.class)
    PlanTotals planTotals(@Param("runDate") LocalDate runDate, @Param("arrivalId") UUID arrivalId);

    /** Ordinal-th boundary sequences: the eligible rows (either flow's arm, SCRUM-69) ranked by original sequence, every maxSize-th. */
    @Query(value = """
            SELECT sequence FROM (
                SELECT m.sequence, row_number() OVER (ORDER BY m.sequence) AS rn
                FROM (
                    SELECT t.sequence
                    FROM cde_schedule s
                    JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence AND v.outcome = 'PASS'
                    JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
                    WHERE s.process_date = :runDate AND s.arrival_id = :arrivalId
                    UNION
                    SELECT t.sequence
                    """ + PAY_MEMBER_ROWS + """
            ) AS m) AS ranked
            WHERE rn % :maxSize = 0 ORDER BY sequence""", rowMapperClass = SequenceRowMapper.class)
    List<Integer> batchBoundaries(@Param("runDate") LocalDate runDate, @Param("arrivalId") UUID arrivalId,
                                  @Param("maxSize") int maxSize);

    /** Freezes the batch's member totals at plan time; R-24 reconciles each file against its OWN frozen values. */
    @Modifying
    @Query("UPDATE crw_emission SET tx_count = :c, control_sum = :s, updated_at = now() WHERE id = :id")
    void freezeTotals(@Param("id") UUID id, @Param("c") long c, @Param("s") BigDecimal s);

    /**
     * Batches already claimed for the parent on OTHER run dates (SCRUM-55
     * review fix): the outbound artifact sequence continues across run dates
     * (a matured-warehoused re-emission is a NEW physical artifact), so bare
     * MsgId and _N identities never repeat within a parent. Runs inside the
     * arrival transaction; committed prior plans are immutable, so the offset
     * is stable for every replay of this (arrival, run_date). crw-5 residual:
     * this count executes in the SAME plan-tx snapshot as the batch selection
     * the caller publishes from, and it is trustworthy ONLY because files are
     * published strictly AFTER their rows commit (durable-effect ordering):
     * committed rows are therefore the COMPLETE artifact registry. Never call
     * this at publication time to (re)derive names; publication uses the
     * stored outbound_msg_id/file_name of the selected unpublished rows.
     */
    @Query("SELECT count(*) FROM crw_emission WHERE arrival_id = :arrivalId AND run_date <> :runDate")
    long countPriorArtifacts(@Param("arrivalId") UUID arrivalId, @Param("runDate") LocalDate runDate);

    /**
     * Snapshot claim (R-24) at batch grain (SCRUM-55): first writer wins on the
     * FULL identity (arrival_id, run_date, batch_ordinal); a restart no-ops and
     * reuses the existing batch row.
     */
    @Modifying
    @Query("""
            INSERT INTO crw_emission (id, group_id, arrival_id, run_date, batch_ordinal,
                                      outbound_msg_id, file_name, state)
            VALUES (:#{#e.id}, :#{#e.groupId}, :#{#e.arrivalId}, :#{#e.runDate}, :#{#e.batchOrdinal},
                    :#{#e.outboundMsgId}, :#{#e.fileName}, :#{#e.state})
            ON CONFLICT (arrival_id, run_date, batch_ordinal) DO NOTHING""")
    void claimSnapshot(@Param("e") CrwEmissionEntity e);

    List<CrwEmissionEntity> findByArrivalIdAndRunDateOrderByBatchOrdinal(UUID arrivalId, LocalDate runDate);

    @Modifying
    @Query("UPDATE crw_emission SET state = :state, updated_at = now() WHERE id = :id AND state <> :state")
    void transition(@Param("id") UUID id, @Param("state") String state);

    /** Ordinal publication (SCRUM-55): the atomic VISIBLE move stamps visible_at for SLA timers. */
    @Modifying
    @Query("UPDATE crw_emission SET state = 'VISIBLE', visible_at = now(), updated_at = now()"
            + " WHERE id = :id AND state <> 'VISIBLE'")
    void markVisible(@Param("id") UUID id);
}
