package za.co.fnb.dcre.crw.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.model.DueRow;
import za.co.fnb.dcre.crw.data.model.FuturedCountRow;
import za.co.fnb.dcre.crw.data.model.FuturedRow;
import za.co.fnb.dcre.crw.data.model.PlanTotals;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface CrwEmissionRepo extends CrudRepository<CrwEmissionEntity, UUID> {

    /**
     * Arrivals with at least one transaction scheduled for the run date
     * (SCRUM-42 load fix): scalars only, one row per arrival, no per-tx
     * fanout. The 23x300k whole-backlog join blew CRDB's sql memory budget
     * (joinreader-mem) live, so per-tx rows are fetched per arrival in
     * {@link #findDueForArrival}. Validation is checked there too: an arrival
     * whose due rows all failed validation lists here, fetches empty, and is
     * skipped without a claim (same net outcome as the old joined query).
     */
    @Query(value = """
            SELECT DISTINCT s.arrival_id, h.initg_pty, h.msg_id
            FROM cde_schedule s
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate
            ORDER BY s.arrival_id""", rowMapperClass = DueArrivalRowMapper.class)
    List<DueArrivalRow> findDueArrivals(@Param("runDate") LocalDate runDate);

    /** ONE arrival's transactions due on the run date (R-37): PASS verdict + schedule hits the run date. */
    @Query(value = """
            SELECT t.arrival_id, h.initg_pty, h.msg_id, t.sequence, t.e2e, t.amount
            FROM cde_schedule s
            JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence AND v.outcome = 'PASS'
            JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate AND s.arrival_id = :arrivalId
            ORDER BY t.sequence""", rowMapperClass = DueRowMapper.class)
    List<DueRow> findDueForArrival(@Param("runDate") LocalDate runDate, @Param("arrivalId") UUID arrivalId);

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
     * share one CRDB snapshot and the due-set cannot shift mid-plan.
     */
    @Query(value = """
            SELECT count(*) AS total_tx, COALESCE(sum(t.amount), 0) AS total_amount
            FROM cde_schedule s
            JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence AND v.outcome = 'PASS'
            JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
            WHERE s.process_date = :runDate AND s.arrival_id = :arrivalId""",
            rowMapperClass = PlanTotalsRowMapper.class)
    PlanTotals planTotals(@Param("runDate") LocalDate runDate, @Param("arrivalId") UUID arrivalId);

    /** Ordinal-th boundary sequences: the eligible rows ranked by original sequence, every maxSize-th. */
    @Query(value = """
            SELECT sequence FROM (
                SELECT t.sequence, row_number() OVER (ORDER BY t.sequence) AS rn
                FROM cde_schedule s
                JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence AND v.outcome = 'PASS'
                JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
                WHERE s.process_date = :runDate AND s.arrival_id = :arrivalId) AS ranked
            WHERE rn % :maxSize = 0 ORDER BY sequence""", rowMapperClass = SequenceRowMapper.class)
    List<Integer> batchBoundaries(@Param("runDate") LocalDate runDate, @Param("arrivalId") UUID arrivalId,
                                  @Param("maxSize") int maxSize);

    /** Freezes the batch's member totals at plan time; R-24 reconciles each file against its OWN frozen values. */
    @Modifying
    @Query("UPDATE crw_emission SET tx_count = :c, control_sum = :s, updated_at = now() WHERE id = :id")
    void freezeTotals(@Param("id") UUID id, @Param("c") long c, @Param("s") BigDecimal s);

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

    Optional<CrwEmissionEntity> findByArrivalIdAndRunDate(UUID arrivalId, LocalDate runDate);

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
