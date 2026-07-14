package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.RowMapper;
import za.co.fnb.dcre.crw.data.model.FuturedCountRow;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.UUID;

public class FuturedCountRowMapper implements RowMapper<FuturedCountRow> {

    @Override
    public FuturedCountRow mapRow(ResultSet r, int rowNum) throws SQLException {
        return new FuturedCountRow(r.getObject("arrival_id", UUID.class),
                r.getObject("process_date", LocalDate.class), r.getLong("futured"));
    }
}
