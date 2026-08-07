package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;
import za.co.fnb.dcre.crw.data.model.FuturedCountRow;
import za.co.fnb.dcre.crw.data.model.PlanTotals;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * A-78: composes each due query from the arms this database can resolve.
 *
 * <p>With both arms present the composed SQL is the original UNION, unchanged. With one arm's
 * tables absent that arm is omitted and the other still runs, instead of PostgreSQL failing the
 * whole statement on an unresolvable relation. With neither, the result is empty: bootstrap
 * ordering is no work, not a failure.
 */
public class CrwDueQueriesImpl implements CrwDueQueries {

    private final NamedParameterJdbcTemplate jdbc;

    private final DueArms arms;

    public CrwDueQueriesImpl(final NamedParameterJdbcTemplate jdbc, final DueArms arms) {
        this.jdbc = jdbc;
        this.arms = arms;
    }

    @Override
    public List<DueArrivalRow> findDueArrivals(final LocalDate runDate) {
        return queryArms(DueSql.DC_ARRIVALS, DueSql.PAY_ARRIVALS, "\nORDER BY arrival_id",
                params(runDate), new DueArrivalRowMapper());
    }

    @Override
    public List<DueArrivalRow> findDueArrivals(final LocalDate runDate, final String client) {
        return queryArms(DueSql.DC_ARRIVALS_FOR_CLIENT, DueSql.PAY_ARRIVALS_FOR_CLIENT,
                "\nORDER BY created_at, arrival_id",
                params(runDate).addValue("client", client), new DueArrivalRowMapper());
    }

    @Override
    public List<String> findDueClients(final LocalDate runDate) {
        return queryArms(DueSql.DC_CLIENTS, DueSql.PAY_CLIENTS, "\nORDER BY initg_pty",
                params(runDate), new ClientRowMapper());
    }

    @Override
    public List<FuturedCountRow> findFuturedCounts(final LocalDate runDate) {
        return arms.dc()
                ? jdbc.query(DueSql.FUTURED_COUNTS, params(runDate), new FuturedCountRowMapper())
                : List.of();
    }

    @Override
    public List<FuturedCountRow> findFuturedCounts(final LocalDate runDate, final String client) {
        return arms.dc()
                ? jdbc.query(DueSql.FUTURED_COUNTS_FOR_CLIENT,
                        params(runDate).addValue("client", client), new FuturedCountRowMapper())
                : List.of();
    }

    /**
     * UNION on (sequence, amount) keeps a row single-counted even if both arms ever matched the
     * same parent, exactly as the original single statement did.
     */
    @Override
    public PlanTotals planTotals(final LocalDate runDate, final UUID arrivalId) {
        final String members = arms.union("SELECT t.sequence, t.amount " + DueSql.DC_MEMBER_ROWS,
                "SELECT t.sequence, t.amount " + DueSql.PAY_MEMBER_ROWS);
        if (members.isEmpty()) {
            return new PlanTotals(0, BigDecimal.ZERO);
        }
        return jdbc.queryForObject("SELECT count(*) AS total_tx, COALESCE(sum(m.amount), 0) AS total_amount"
                        + " FROM (\n" + members + "\n) AS m",
                params(runDate).addValue("arrivalId", arrivalId), new PlanTotalsRowMapper());
    }

    @Override
    public List<Integer> batchBoundaries(final LocalDate runDate, final UUID arrivalId, final int maxSize) {
        final String members = arms.union("SELECT t.sequence " + DueSql.DC_MEMBER_ROWS,
                "SELECT t.sequence " + DueSql.PAY_MEMBER_ROWS);
        if (members.isEmpty()) {
            return List.of();
        }
        return jdbc.query("SELECT sequence FROM (SELECT m.sequence,"
                        + " row_number() OVER (ORDER BY m.sequence) AS rn FROM (\n" + members
                        + "\n) AS m) AS ranked WHERE rn % :maxSize = 0 ORDER BY sequence",
                params(runDate).addValue("arrivalId", arrivalId).addValue("maxSize", maxSize),
                new SequenceRowMapper());
    }

    private <T> List<T> queryArms(final String dcArm, final String payArm, final String orderBy,
            final MapSqlParameterSource p, final org.springframework.jdbc.core.RowMapper<T> mapper) {
        final String sql = arms.union(dcArm, payArm);
        return sql.isEmpty() ? List.of() : jdbc.query(sql + orderBy, p, mapper);
    }

    private MapSqlParameterSource params(final LocalDate runDate) {
        return new MapSqlParameterSource("runDate", runDate);
    }
}
