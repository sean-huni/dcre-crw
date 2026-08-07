package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.time.LocalDate;
import java.util.UUID;

/**
 * A-78: composes the member claim from the arms this database can resolve, exactly as
 * {@link CrwDueQueriesImpl} does for the due queries.
 *
 * <p>With both arms present the statement is the original UNION, unchanged.
 */
public class CrwMemberClaimsImpl implements CrwMemberClaims {

    private static final String MEMBER_COLUMNS = "SELECT t.sequence, t.e2e, t.amount ";

    private final NamedParameterJdbcTemplate jdbc;

    private final DueArms arms;

    public CrwMemberClaimsImpl(final NamedParameterJdbcTemplate jdbc, final DueArms arms) {
        this.jdbc = jdbc;
        this.arms = arms;
    }

    @Override
    public void claimMembers(final UUID emissionId, final LocalDate runDate, final UUID arrivalId,
            final int loSeq, final int hiSeq) {
        final String members = arms.union(MEMBER_COLUMNS + DueSql.DC_MEMBER_ROWS,
                MEMBER_COLUMNS + DueSql.PAY_MEMBER_ROWS);
        if (members.isEmpty()) {
            return;
        }
        jdbc.update("""
                INSERT INTO crw_emission_member (id, emission_id, sequence, e2e, amount)
                SELECT gen_random_uuid(), :emissionId, m.sequence, m.e2e, m.amount FROM (
                """ + members + """
                ) AS m
                WHERE m.sequence >= :loSeq AND (:hiSeq = -1 OR m.sequence <= :hiSeq)
                ON CONFLICT (emission_id, sequence) DO NOTHING""",
                new MapSqlParameterSource("emissionId", emissionId)
                        .addValue("runDate", runDate)
                        .addValue("arrivalId", arrivalId)
                        .addValue("loSeq", loSeq)
                        .addValue("hiSeq", hiSeq));
    }
}
