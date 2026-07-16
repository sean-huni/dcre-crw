package za.co.fnb.dcre.crw.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.crw.data.model.CrwEmissionGroupEntity;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

public interface CrwEmissionGroupRepo extends CrudRepository<CrwEmissionGroupEntity, UUID> {

    Optional<CrwEmissionGroupEntity> findByArrivalIdAndRunDate(UUID arrivalId, LocalDate runDate);

    /** Plan claim (R-24 shape): first writer wins; a replay finds the stored plan untouched. */
    @Modifying
    @Query("""
            INSERT INTO crw_emission_group (id, arrival_id, client, source_msg_id, run_date, applied_max,
                                            total_tx, total_amount, expected_batch_count, split)
            VALUES (:#{#g.id}, :#{#g.arrivalId}, :#{#g.client}, :#{#g.sourceMsgId}, :#{#g.runDate},
                    :#{#g.appliedMax}, :#{#g.totalTx}, :#{#g.totalAmount}, :#{#g.expectedBatchCount}, :#{#g.split})
            ON CONFLICT (arrival_id, run_date) DO NOTHING""")
    void claimPlan(@Param("g") CrwEmissionGroupEntity g);
}
