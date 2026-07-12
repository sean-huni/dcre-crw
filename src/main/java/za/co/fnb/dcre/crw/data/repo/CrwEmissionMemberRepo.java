package za.co.fnb.dcre.crw.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.crw.data.model.CrwEmissionMemberEntity;

import java.util.List;
import java.util.UUID;

public interface CrwEmissionMemberRepo extends CrudRepository<CrwEmissionMemberEntity, UUID> {

    @Modifying
    @Query("""
            INSERT INTO crw_emission_member (emission_id, sequence, e2e, amount)
            VALUES (:#{#e.emissionId}, :#{#e.sequence}, :#{#e.e2e}, :#{#e.amount})
            ON CONFLICT (emission_id, sequence) DO NOTHING""")
    void addMember(@Param("e") CrwEmissionMemberEntity e);

    List<CrwEmissionMemberEntity> findByEmissionIdOrderBySequence(UUID emissionId);
}
