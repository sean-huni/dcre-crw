package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.time.LocalDate;
import java.util.UUID;

/**
 * The set-based member claim over the collections lane, as one plain statement.
 *
 * <p>This was the FOURTH site carrying the two-arm UNION, found by the compiler rather than by
 * inspection when the shared fragment moved. The payments arm is PRW's, so there is one arm and
 * no composition left to do here.
 */
public class CrwMemberClaimsImpl implements CrwMemberClaims {

    private static final String MEMBERS = "SELECT t.sequence, t.e2e, t.amount " + DueSql.MEMBER_ROWS;

    private final NamedParameterJdbcTemplate jdbc;

    public CrwMemberClaimsImpl(final NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void claimMembers(final UUID emissionId, final LocalDate runDate, final UUID arrivalId,
            final int loSeq, final int hiSeq) {
        jdbc.update("""
                INSERT INTO crw_emission_member (id, emission_id, sequence, e2e, amount)
                SELECT gen_random_uuid(), :emissionId, m.sequence, m.e2e, m.amount FROM (
                """ + MEMBERS + """
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
