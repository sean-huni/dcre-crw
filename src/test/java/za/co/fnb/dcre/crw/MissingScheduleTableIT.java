package za.co.fnb.dcre.crw;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.BadSqlGrammarException;
import za.co.fnb.dcre.crw.service.EmissionService;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The deliberate inversion of A-76/A-78 that comes with CRW being collections-only.
 *
 * <p>A-76 made a missing peer table mean "nothing is due", so a CRW window on a database where
 * CDE had not yet run reported success instead of dying {@code exit 5}. A-78 replaced that with
 * per-arm composition, and it was defensible ONLY because there were two arms: leaving the pay
 * arm out still left the collections arm running, so the degradation could never cover the whole
 * due-set.
 *
 * <p>CRW now has one lane. Guarding it is therefore the A-76 shape again, and A-78 already
 * recorded why that shape is worse than the crash: a collections cluster whose
 * {@code cde_schedule} never appears would report clean, empty windows forever while emitting
 * nothing, and since R-37 was amended to gate DAG_COMPLETE on a CRW emission, every collections
 * arrival would sit in DAG_RUNNING with no error anywhere. A crash is loud. A permanent silent
 * zero is not.
 *
 * <p>So this suite asserts the OPPOSITE of what it asserted before: a missing peer table fails
 * the window, and it fails naming the table, not merely non-zero. An assertion of "throws
 * something" would also pass if the context failed to start.
 */
class MissingScheduleTableIT extends CrwTestcontainersBase {

    @Autowired
    private EmissionService emissions;

    /** Every peer table back, so suites stay order-independent whichever one a test dropped. */
    @AfterEach
    void restoreEveryPeerTable() {
        ensureSpineTables();
    }

    @Test
    void anAbsentScheduleTableFailsTheWindowLoudly() {
        dropWithDependents("cde_schedule");

        assertThatThrownBy(() -> emissions.emitDue(LocalDate.now()))
                .as("degrading to nothing-due here is a permanent silent zero: CRW would report"
                        + " success while every collections arrival hangs in DAG_RUNNING")
                .isInstanceOf(BadSqlGrammarException.class)
                .rootCause().hasMessageContaining(missing("cde_schedule"));
    }

    @Test
    void theClientScopedLaneFailsTheSameWay() {
        dropWithDependents("cde_schedule");

        assertThatThrownBy(() -> emissions.emitDue(LocalDate.now(), "FNBCC01"))
                .as("the per-lane overload takes the same due query and must fail identically")
                .isInstanceOf(BadSqlGrammarException.class)
                .rootCause().hasMessageContaining(missing("cde_schedule"));
    }

    /**
     * The partitioned job calls this BEFORE either emitDue overload, so it is the site a fresh
     * database reaches first and the one a guard applied only to emitDue would leave silent.
     */
    @Test
    void theLaneUniverseQueryFailsToo() {
        dropWithDependents("cde_schedule");

        assertThatThrownBy(() -> emissions.dueClients(LocalDate.now()))
                .isInstanceOf(BadSqlGrammarException.class)
                .rootCause().hasMessageContaining(missing("cde_schedule"));
    }

    /**
     * Hunt the class, not the instance. The due path joins three tables owned by three different
     * services and a fresh database has none of them, so all three must fail the same way and
     * each must name ITSELF: a sweep asserting only the exception type would pass while every
     * case died on the first table dropped.
     */
    @Test
    void anyOneMissingPeerTableFailsTheWindowNamingItself() {
        for (final String table : new String[]{"cde_schedule", "validation_log", "tx_header"}) {
            ensureSpineTables();
            dropWithDependents(table);

            assertThatThrownBy(() -> emissions.emitDue(LocalDate.now()))
                    .as("a missing %s must fail the window, not silently empty it", table)
                    .isInstanceOf(BadSqlGrammarException.class)
                    .rootCause().hasMessageContaining(missing(table));
        }
    }

    /**
     * The ROOT cause text, not the wrapper's. BadSqlGrammarException embeds the whole failing
     * statement in its message, and every one of these statements NAMES all three tables, so
     * asserting the table name against the wrapper passes whichever relation was actually
     * missing: one test, three reasons to go green.
     */
    private String missing(final String table) {
        return "relation \"" + table + "\" does not exist";
    }

    /**
     * CASCADE, because crw_emission_owed (SCRUM-107) is defined over these tables and CockroachDB
     * refuses to drop a table a view depends on. EmissionOwedViewIT recreates the view in its own
     * setup, so the two suites stay order-independent.
     */
    private void dropWithDependents(final String table) {
        jdbc.execute("DROP TABLE IF EXISTS " + table + " CASCADE");
    }
}
