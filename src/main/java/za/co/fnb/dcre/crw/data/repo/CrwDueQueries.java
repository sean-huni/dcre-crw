package za.co.fnb.dcre.crw.data.repo;

import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.model.FuturedCountRow;
import za.co.fnb.dcre.crw.data.model.PlanTotals;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * The due-path queries, as a repository fragment rather than {@code @Query} annotations (A-78).
 *
 * <p>They are the only queries in CRW that span BOTH the DC and pay arms, so they are the only
 * ones whose SQL has to be composed from the arms this database can actually resolve. Everything
 * else in {@link CrwEmissionRepo} touches CRW's own tables and stays declarative.
 *
 * <p>Signatures are unchanged from the annotated versions they replace, so callers and their
 * tests are untouched.
 */
public interface CrwDueQueries {

    /** Arrivals with work due on the run date, either arm, ordered by arrival id. */
    List<DueArrivalRow> findDueArrivals(LocalDate runDate);

    /**
     * One client lane's due parents, FIFO by eligibility order: tx_header insertion order
     * (created_at), arrival id as the deterministic tie-break. The ordering spans BOTH arms, so
     * DC and pay parents interleave in one FIFO sequence (SCRUM-69).
     */
    List<DueArrivalRow> findDueArrivals(LocalDate runDate, String client);

    /** The lane universe for the partitioner: distinct clients with work due, either arm. */
    List<String> findDueClients(LocalDate runDate);

    /** Warehoused counts per (arrival, process date) past the run date. DC arm only. */
    List<FuturedCountRow> findFuturedCounts(LocalDate runDate);

    /** Client-scoped warehoused counts, so parallel lanes never duplicate an R-38 WARN. */
    List<FuturedCountRow> findFuturedCounts(LocalDate runDate, String client);

    /**
     * Whole-parent totals for the frozen plan (SCRUM-55): executes inside the caller's arrival
     * transaction, so totals, boundaries and member claims share one CRDB snapshot and the
     * due-set cannot shift mid-plan.
     */
    PlanTotals planTotals(LocalDate runDate, UUID arrivalId);

    /** Ordinal-th boundary sequences: eligible rows ranked by original sequence, every maxSize-th. */
    List<Integer> batchBoundaries(LocalDate runDate, UUID arrivalId, int maxSize);
}
