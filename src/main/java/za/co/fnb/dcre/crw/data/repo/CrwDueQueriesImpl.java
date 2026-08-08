package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.model.FuturedCountRow;
import za.co.fnb.dcre.crw.data.model.PlanTotals;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * The due-path queries as plain statements over the collections lane.
 *
 * <p>Every query here used to be composed at runtime from a collections arm and a payments arm,
 * and a one-armed UNION is not a UNION. The payments lane belongs to PRW, so the composition,
 * the arm-presence probe and the empty-composition short-circuits are gone with it; see
 * {@link DueSql} for why no presence guard replaced them.
 */
public class CrwDueQueriesImpl implements CrwDueQueries {

    private final NamedParameterJdbcTemplate jdbc;

    public CrwDueQueriesImpl(final NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public List<DueArrivalRow> findDueArrivals(final LocalDate runDate) {
        return jdbc.query(DueSql.ARRIVALS + "\nORDER BY arrival_id", params(runDate),
                new DueArrivalRowMapper());
    }

    @Override
    public List<DueArrivalRow> findDueArrivals(final LocalDate runDate, final String client) {
        return jdbc.query(DueSql.ARRIVALS_FOR_CLIENT + "\nORDER BY created_at, arrival_id",
                params(runDate).addValue("client", client), new DueArrivalRowMapper());
    }

    @Override
    public List<String> findDueClients(final LocalDate runDate) {
        return jdbc.query(DueSql.CLIENTS + "\nORDER BY initg_pty", params(runDate), new ClientRowMapper());
    }

    @Override
    public List<FuturedCountRow> findFuturedCounts(final LocalDate runDate) {
        return jdbc.query(DueSql.FUTURED_COUNTS, params(runDate), new FuturedCountRowMapper());
    }

    @Override
    public List<FuturedCountRow> findFuturedCounts(final LocalDate runDate, final String client) {
        return jdbc.query(DueSql.FUTURED_COUNTS_FOR_CLIENT,
                params(runDate).addValue("client", client), new FuturedCountRowMapper());
    }

    @Override
    public PlanTotals planTotals(final LocalDate runDate, final UUID arrivalId) {
        return jdbc.queryForObject("SELECT count(*) AS total_tx, COALESCE(sum(m.amount), 0) AS total_amount"
                        + " FROM (\nSELECT t.sequence, t.amount " + DueSql.MEMBER_ROWS + "\n) AS m",
                params(runDate).addValue("arrivalId", arrivalId), new PlanTotalsRowMapper());
    }

    @Override
    public List<Integer> batchBoundaries(final LocalDate runDate, final UUID arrivalId, final int maxSize) {
        return jdbc.query("SELECT sequence FROM (SELECT m.sequence,"
                        + " row_number() OVER (ORDER BY m.sequence) AS rn FROM (\nSELECT t.sequence "
                        + DueSql.MEMBER_ROWS + "\n) AS m) AS ranked WHERE rn % :maxSize = 0 ORDER BY sequence",
                params(runDate).addValue("arrivalId", arrivalId).addValue("maxSize", maxSize),
                new SequenceRowMapper());
    }

    private MapSqlParameterSource params(final LocalDate runDate) {
        return new MapSqlParameterSource("runDate", runDate);
    }
}
