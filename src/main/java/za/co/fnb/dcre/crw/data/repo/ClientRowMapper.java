package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ClientRowMapper implements RowMapper<String> {

    @Override
    public String mapRow(final ResultSet r, final int rowNum) throws SQLException {
        return r.getString("initg_pty");
    }
}
