package za.co.fnb.dcre.crw.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.FuturedRow;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

public interface CrwEmissionRepo extends CrudRepository<CrwEmissionEntity, UUID>, CrwDueQueries {

    /**
     * ONE (arrival, process date) group's warehoused rows, for per-tx R-38 WARN detail on small
     * groups.
     *
     * <p>The last statement in CRW naming a peer table that is NOT composed per arm (A-78). It is
     * safe because it is unreachable without one: the only caller iterates the groups returned by
     * findFuturedCounts, which is DC-arm-gated and returns empty when cde_schedule is absent, so
     * there is no group to ask about. That is a structural guarantee, not proximity. Calling it
     * directly on a database without cde_schedule WILL throw.
     */
    @Query(value = """
            SELECT t.arrival_id, t.sequence, t.e2e, s.process_date
            FROM cde_schedule s
            JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence AND v.outcome = 'PASS'
            JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
            WHERE s.arrival_id = :arrivalId AND s.process_date = :processDate
            ORDER BY t.sequence""", rowMapperClass = FuturedRowMapper.class)
    List<FuturedRow> findFuturedForArrival(@Param("arrivalId") UUID arrivalId,
                                           @Param("processDate") LocalDate processDate);

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
