package za.co.fnb.dcre.crw.data.repo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The due statements are ASSEMBLED from a text block and {@link DueSql#CLIENT_EXPR}, and a Java
 * text block strips trailing whitespace from every line. On 2026-08-08 that produced
 * {@code ...:runDate ANDCOALESCE(h.client_token, h.initg_pty) = ?} in two shipped statements, which
 * CockroachDB rejected as bad grammar at runtime and no compiler could see.
 *
 * <p>These assertions are on the assembled string, not on the fragments, because the fragments were
 * individually correct. They cost no database and they fail in milliseconds, which is the whole
 * point: the integration tests that caught it needed a container and a seeded arrival.
 */
class DueSqlAssemblyTest {

    @Test
    void everyClientPredicateKeepsItsSeparator() {
        assertThat(DueSql.ARRIVALS_FOR_CLIENT).contains(" AND COALESCE(h.client_token, h.initg_pty) = :client");
        assertThat(DueSql.FUTURED_COUNTS_FOR_CLIENT)
                .contains(" AND COALESCE(h.client_token, h.initg_pty) = :client");
    }

    @Test
    void everyClientProjectionIsAliasedSoTheRowMappersAndOrderByCanNameIt() {
        assertThat(DueSql.ARRIVALS).contains("COALESCE(h.client_token, h.initg_pty) AS client");
        assertThat(DueSql.ARRIVALS_FOR_CLIENT).contains("COALESCE(h.client_token, h.initg_pty) AS client");
        assertThat(DueSql.CLIENTS).contains("COALESCE(h.client_token, h.initg_pty) AS client");
    }

    /**
     * A-43: no due statement may read {@code initg_pty} except through the fallback arm. A bare
     * reference would be a second, unaliased authority, which is the two-homes shape this ruling
     * removes.
     */
    @Test
    void noDueStatementReadsTheHeaderColumnOutsideTheFallback() {
        for (final String sql : new String[] {DueSql.ARRIVALS, DueSql.ARRIVALS_FOR_CLIENT, DueSql.CLIENTS,
                DueSql.FUTURED_COUNTS, DueSql.FUTURED_COUNTS_FOR_CLIENT, DueSql.MEMBER_ROWS}) {
            assertThat(sql.replace(DueSql.CLIENT_EXPR, ""))
                    .as("statement reads initg_pty outside COALESCE: %s", sql)
                    .doesNotContain("initg_pty");
        }
    }
}
