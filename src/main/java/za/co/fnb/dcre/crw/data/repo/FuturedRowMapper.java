package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.RowMapper;
import za.co.fnb.dcre.crw.data.model.FuturedRow;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.UUID;

public class FuturedRowMapper implements RowMapper<FuturedRow> {

    @Override
    public FuturedRow mapRow(ResultSet r, int rowNum) throws SQLException {
        return new FuturedRow(r.getObject("arrival_id", UUID.class), r.getInt("sequence"),
                r.getString("e2e"), r.getObject("process_date", LocalDate.class));
    }
}
