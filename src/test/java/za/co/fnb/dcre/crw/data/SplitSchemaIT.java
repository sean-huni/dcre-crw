package za.co.fnb.dcre.crw.data;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import za.co.fnb.dcre.crw.CrwTestcontainersBase;
import za.co.fnb.dcre.crw.data.model.CrwEmissionEntity;
import za.co.fnb.dcre.crw.data.model.CrwEmissionGroupEntity;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionGroupRepo;
import za.co.fnb.dcre.crw.data.repo.CrwEmissionRepo;

import javax.xml.parsers.DocumentBuilderFactory;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
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
        var g = CrwEmissionGroupEntity.planned(arrival, "FNBRF01", "DCRERF2026071600000900",
                LocalDate.of(2026, 7, 16), 5000, 12001L, new BigDecimal("120010.00"), 3, true);
        groups.save(g);

        emissions.claimSnapshot(CrwEmissionEntity.plannedBatch(g.getId(), arrival,
                g.getRunDate(), 1, "DCRERF2026071600000900_1", "FNBRF01_DCRERF2026071600000900_1_PAIN008.xml"));
        emissions.claimSnapshot(CrwEmissionEntity.plannedBatch(g.getId(), arrival,
                g.getRunDate(), 2, "DCRERF2026071600000900_2", "FNBRF01_DCRERF2026071600000900_2_PAIN008.xml"));
        // same ordinal again = restart no-op, not a violation
        emissions.claimSnapshot(CrwEmissionEntity.plannedBatch(g.getId(), arrival,
                g.getRunDate(), 2, "DCRERF2026071600000900_2", "FNBRF01_DCRERF2026071600000900_2_PAIN008.xml"));

        assertThat(emissions.findByArrivalIdAndRunDateOrderByBatchOrdinal(arrival, g.getRunDate())).hasSize(2);
        assertThat(groups.findByArrivalIdAndRunDate(arrival, g.getRunDate())).isPresent();
    }

    /**
     * Legacy-state migration regression (SCRUM-55 review BLOCKER, migration
     * blast radius): a legacy multi-date arrival (one pre-split emission per
     * run date, identical bare filename, no group yet) must backfill with
     * DISTINCT outbound ids per run date (bare for the earliest artifact,
     * _N continuation after), or the 003-split-backfill UPDATE violates
     * uq_emission_outbound_msg and the migration itself fails at startup.
     * Executes the changeset's REAL SQL body, not a copy.
     */
    @Test
    void backfillAssignsDistinctOutboundIdsToLegacyMultiDateArrivals() throws Exception {
        UUID arrival = UUID.randomUUID();
        jdbc.update("INSERT INTO crw_emission (arrival_id, run_date, file_name, state) VALUES (?,?,?,?)",
                arrival, LocalDate.of(2026, 6, 1), "FNBLG01_DCRELEGACY0000000000901_PAIN008.xml", "VISIBLE");
        jdbc.update("INSERT INTO crw_emission (arrival_id, run_date, file_name, state) VALUES (?,?,?,?)",
                arrival, LocalDate.of(2026, 6, 8), "FNBLG01_DCRELEGACY0000000000901_PAIN008.xml", "VISIBLE");

        jdbc.execute(backfillSql()); // must not violate uq_emission_outbound_msg

        List<String> outbound = jdbc.queryForList("SELECT outbound_msg_id FROM crw_emission"
                + " WHERE arrival_id = ? ORDER BY run_date", String.class, arrival);
        assertThat(outbound).containsExactly("DCRELEGACY0000000000901", "DCRELEGACY0000000000901_2");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM crw_emission_group WHERE arrival_id = ?",
                Integer.class, arrival)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM crw_emission WHERE arrival_id = ?"
                + " AND group_id IS NULL", Integer.class, arrival)).isZero();
    }

    /** The 003-split-backfill changeset's SQL body, straight from the shipped changelog. */
    private String backfillSql() throws Exception {
        var doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(getClass().getResourceAsStream("/db/changelog/2026/07/003-split.xml"));
        NodeList changeSets = doc.getElementsByTagName("changeSet");
        for (int i = 0; i < changeSets.getLength(); i++) {
            Element changeSet = (Element) changeSets.item(i);
            if ("003-split-backfill".equals(changeSet.getAttribute("id"))) {
                return changeSet.getElementsByTagName("sql").item(0).getTextContent();
            }
        }
        throw new IllegalStateException("003-split-backfill changeset not found in 003-split.xml");
    }
}
