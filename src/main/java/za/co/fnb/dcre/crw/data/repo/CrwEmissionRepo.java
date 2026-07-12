package za.co.fnb.dcre.crw.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.DueRow;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface CrwEmissionRepo extends CrudRepository<CrwEmissionEntity, UUID> {

    /** Transactions due today (R-37): PASS verdict + schedule hits the run date. */
    @Query(value = """
            SELECT t.arrival_id, h.initg_pty, h.msg_id, t.sequence, t.e2e, t.amount
            FROM cde_schedule s
            JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence AND v.outcome = 'PASS'
            JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
            JOIN tx_header h ON h.arrival_id = s.arrival_id
            WHERE s.process_date = :runDate
            ORDER BY t.arrival_id, t.sequence""", rowMapperClass = DueRowMapper.class)
    List<DueRow> findDue(@Param("runDate") LocalDate runDate);

    /** Snapshot claim (R-24): first writer wins; a restart sees empty and reuses the existing snapshot. */
    @Modifying
    @Query("""
            INSERT INTO crw_emission (id, arrival_id, run_date, file_name, state)
            VALUES (:#{#e.id}, :#{#e.arrivalId}, :#{#e.runDate}, :#{#e.fileName}, :#{#e.state})
            ON CONFLICT (arrival_id, run_date) DO NOTHING""")
    void claimSnapshot(@Param("e") CrwEmissionEntity e);

    Optional<CrwEmissionEntity> findByArrivalIdAndRunDate(UUID arrivalId, LocalDate runDate);

    @Modifying
    @Query("UPDATE crw_emission SET state = :state, updated_at = now() WHERE id = :id AND state <> :state")
    void transition(@Param("id") UUID id, @Param("state") String state);
}
