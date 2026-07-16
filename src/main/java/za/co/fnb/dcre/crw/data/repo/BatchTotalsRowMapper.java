package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.RowMapper;
import za.co.fnb.dcre.crw.data.model.BatchTotals;

import java.sql.ResultSet;
import java.sql.SQLException;

public class BatchTotalsRowMapper implements RowMapper<BatchTotals> {

    @Override
    public BatchTotals mapRow(final ResultSet r, final int rowNum) throws SQLException {
        return new BatchTotals(r.getLong("c"), r.getBigDecimal("s"));
    }
}
