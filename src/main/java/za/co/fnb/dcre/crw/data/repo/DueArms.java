package za.co.fnb.dcre.crw.data.repo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A-78 (SCRUM-107): which of the two due-query arms can be resolved against THIS database.
 *
 * <p>The due queries are a UNION of a DC arm over {@code cde_schedule} (CDE's) and a pay arm
 * over {@code ais_verdict} (AIS's). PostgreSQL resolves every relation a statement names, so one
 * absent table kills the whole statement, including the arm that could have run. Nine col-crw
 * windows died {@code exit 5} on {@code relation "ais_verdict" does not exist} on a
 * collections-only database.
 *
 * <p>Guarding the whole due-set instead, as A-76 did, is worse than the crash: a
 * collections-only cluster NEVER runs AIS, so that guard would report a clean run while emitting
 * nothing for DC forever. A crash is loud; a permanent silent zero is not.
 *
 * <p>So an arm whose tables are absent is simply left out of the composed SQL, and the other arm
 * still runs. A dependency that has not bootstrapped yet is no work, not a failure, and not a
 * reason to stop unrelated work.
 *
 * <p>Deliberately NOT cached. The probe is one metadata query, and a cache would keep an arm
 * switched off for the life of a context after its table appeared.
 */
@Component
public class DueArms {

    private static final Logger log = LoggerFactory.getLogger(DueArms.class);

    /** Read by BOTH arms: the spine (CRR) and the validation outcomes (CTV). */
    private static final Set<String> BASE = Set.of("tx_header", "tx_entry", "validation_log");

    /** The DC arm additionally needs CDE's schedule. */
    static final Set<String> DC_TABLES = with(BASE, "cde_schedule");

    /** The pay arm additionally needs AIS's verdicts. */
    static final Set<String> PAY_TABLES = with(BASE, "ais_verdict");

    private final JdbcTemplate jdbc;

    public DueArms(final JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean dc() {
        return allPresent(DC_TABLES);
    }

    public boolean pay() {
        return allPresent(PAY_TABLES);
    }

    /**
     * Joins the arms this database can actually resolve. An empty result means neither arm is
     * available, which callers must treat as "nothing due" rather than running a bare ORDER BY.
     */
    public String union(final String dcArm, final String payArm) {
        final List<String> arms = new java.util.ArrayList<>(2);
        if (dc()) {
            arms.add(dcArm);
        }
        if (pay()) {
            arms.add(payArm);
        }
        if (arms.isEmpty()) {
            log.warn("neither CRW due arm is resolvable on this database: the DC arm needs {} and"
                    + " the pay arm needs {}. CRR/CTV/CDE/AIS have not run here, so nothing is"
                    + " due. Bootstrap ordering, not a failure.", DC_TABLES, PAY_TABLES);
        }
        return String.join("\nUNION\n", arms);
    }

    /**
     * The count is compared against the SET's own size rather than a literal, so the names and
     * the number they are checked against cannot drift apart.
     */
    private boolean allPresent(final Set<String> names) {
        final String in = names.stream().map(n -> "'" + n + "'").collect(Collectors.joining(","));
        final Long found = jdbc.queryForObject("SELECT count(*) FROM information_schema.tables"
                + " WHERE table_schema = current_schema() AND table_name IN (" + in + ")", Long.class);
        return found != null && found == names.size();
    }

    private static Set<String> with(final Set<String> base, final String extra) {
        final Set<String> all = new LinkedHashSet<>(base);
        all.add(extra);
        return Set.copyOf(all);
    }
}
