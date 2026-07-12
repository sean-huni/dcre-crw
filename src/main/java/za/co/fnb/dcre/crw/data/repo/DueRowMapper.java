package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.RowMapper;
import za.co.fnb.dcre.crw.data.model.DueRow;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class DueRowMapper implements RowMapper<DueRow> {

    @Override
    public DueRow mapRow(ResultSet r, int rowNum) throws SQLException {
        return new DueRow(r.getObject("arrival_id", UUID.class), r.getString("initg_pty"),
                r.getString("msg_id"), r.getInt("sequence"), r.getString("e2e"),
                r.getBigDecimal("amount"));
    }
}
