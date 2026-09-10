package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reads the {@code client} column, which is {@link DueSql#CLIENT_EXPR} rather than a physical
 * column. The lane partitioner MUST key on the same authority the emission group is written
 * from, or a lane would be selected on one client identity and emit under another (A-43).
 */
public class ClientRowMapper implements RowMapper<String> {

    @Override
    public String mapRow(final ResultSet r, final int rowNum) throws SQLException {
        return r.getString("client");
    }
}
