package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SequenceRowMapper implements RowMapper<Integer> {

    @Override
    public Integer mapRow(final ResultSet r, final int rowNum) throws SQLException {
        return r.getInt("sequence");
    }
}
