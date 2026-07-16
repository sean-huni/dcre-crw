package za.co.fnb.dcre.crw.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.crw.data.model.BatchTotals;
import za.co.fnb.dcre.crw.data.model.CrwEmissionMemberEntity;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

public interface CrwEmissionMemberRepo extends CrudRepository<CrwEmissionMemberEntity, UUID> {

    @Modifying
    @Query("""
            INSERT INTO crw_emission_member (emission_id, sequence, e2e, amount)
            VALUES (:#{#e.emissionId}, :#{#e.sequence}, :#{#e.e2e}, :#{#e.amount})
            ON CONFLICT (emission_id, sequence) DO NOTHING""")
    void addMember(@Param("e") CrwEmissionMemberEntity e);

    /**
     * Set-based ordinal member claim (SCRUM-55): the [loSeq, hiSeq] slice of
     * the eligible rows is claimed in ONE statement inside the arrival
     * transaction; hiSeq -1 means the open-ended tail. No Java list of 300k
     * rows ever (SCRUM-42 memory lesson).
     */
    @Modifying
    @Query("""
            INSERT INTO crw_emission_member (id, emission_id, sequence, e2e, amount)
            SELECT gen_random_uuid(), :emissionId, t.sequence, t.e2e, t.amount
            FROM cde_schedule s
            JOIN validation_log v ON v.arrival_id = s.arrival_id AND v.sequence = s.sequence AND v.outcome = 'PASS'
            JOIN tx_entry t ON t.arrival_id = s.arrival_id AND t.sequence = s.sequence
            WHERE s.process_date = :runDate AND s.arrival_id = :arrivalId
              AND t.sequence >= :loSeq AND (:hiSeq = -1 OR t.sequence <= :hiSeq)
            ON CONFLICT (emission_id, sequence) DO NOTHING""")
    void claimMembers(@Param("emissionId") UUID emissionId, @Param("runDate") LocalDate runDate,
                      @Param("arrivalId") UUID arrivalId, @Param("loSeq") int loSeq, @Param("hiSeq") int hiSeq);

    /** Claimed-member totals for the freeze; count and sum come from the immutable snapshot, not the live spine. */
    @Query(value = "SELECT count(*) AS c, COALESCE(sum(amount), 0) AS s FROM crw_emission_member"
            + " WHERE emission_id = :id", rowMapperClass = BatchTotalsRowMapper.class)
    BatchTotals batchTotals(@Param("id") UUID id);

    List<CrwEmissionMemberEntity> findByEmissionIdOrderBySequence(UUID emissionId);
}
