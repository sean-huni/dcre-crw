package za.co.fnb.dcre.crw.data.repo;

import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.model.FuturedCountRow;
import za.co.fnb.dcre.crw.data.model.PlanTotals;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * The due-path queries, as a repository fragment rather than {@code @Query} annotations.
 *
 * <p>They are the queries whose SQL is assembled from a shared {@link DueSql} fragment, because
 * the member-row predicate is one fact serving four statements. Everything else in
 * {@link CrwEmissionRepo} touches CRW's own tables and stays declarative.
 *
 * <p>Every statement reads {@code cde_schedule}, which CDE owns, and NONE of them guards its
 * presence: a missing schedule table fails the window loudly rather than reporting a clean,
 * empty one forever. {@link DueSql} carries the A-76/A-78 reasoning behind that.
 */
public interface CrwDueQueries {

    /** Arrivals with collections work due on the run date, ordered by arrival id. */
    List<DueArrivalRow> findDueArrivals(LocalDate runDate);

    /**
     * One client lane's due parents, FIFO by eligibility order: tx_header insertion order
     * (created_at), arrival id as the deterministic tie-break.
     */
    List<DueArrivalRow> findDueArrivals(LocalDate runDate, String client);

    /** The lane universe for the partitioner: distinct clients with work due. */
    List<String> findDueClients(LocalDate runDate);

    /** Warehoused counts per (arrival, process date) past the run date. */
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
