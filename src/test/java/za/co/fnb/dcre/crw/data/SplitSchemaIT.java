package za.co.fnb.dcre.crw.data;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import za.co.fnb.dcre.crw.CrwTestcontainersBase;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.CrwEmissionGroupEntity;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionGroupRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionRepo;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SCRUM-55 Task 1: group/batch grain round trip. The emission identity widens
 * to (arrival_id, run_date, batch_ordinal); a same-ordinal replay is a
 * restart no-op, never a violation (003-split changeset).
 */
class SplitSchemaIT extends CrwTestcontainersBase {

    @Autowired
    CrwEmissionGroupRepo groups;

    @Autowired
    CrwEmissionRepo emissions;

    @Test
    void groupAndOrdinalBatchesRoundTrip() {
        UUID arrival = UUID.randomUUID();
        var g = CrwEmissionGroupEntity.planned(arrival, "FNBRF01", "DCRERF2026071600000001",
                LocalDate.of(2026, 7, 16), 5000, 12001L, new BigDecimal("120010.00"), 3, true);
        groups.save(g);

        emissions.claimSnapshot(CrwEmissionEntity.plannedBatch(g.getId(), arrival,
                g.getRunDate(), 1, "DCRERF2026071600000001_1", "FNBRF01_DCRERF2026071600000001_1_PAIN008.xml"));
        emissions.claimSnapshot(CrwEmissionEntity.plannedBatch(g.getId(), arrival,
                g.getRunDate(), 2, "DCRERF2026071600000001_2", "FNBRF01_DCRERF2026071600000001_2_PAIN008.xml"));
        // same ordinal again = restart no-op, not a violation
        emissions.claimSnapshot(CrwEmissionEntity.plannedBatch(g.getId(), arrival,
                g.getRunDate(), 2, "DCRERF2026071600000001_2", "FNBRF01_DCRERF2026071600000001_2_PAIN008.xml"));

        assertThat(emissions.findByArrivalIdAndRunDateOrderByBatchOrdinal(arrival, g.getRunDate())).hasSize(2);
        assertThat(groups.findByArrivalIdAndRunDate(arrival, g.getRunDate())).isPresent();
    }
}
