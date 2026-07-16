package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.RowMapper;
import za.co.fnb.dcre.crw.data.model.PlanTotals;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PlanTotalsRowMapper implements RowMapper<PlanTotals> {

    @Override
    public PlanTotals mapRow(final ResultSet r, final int rowNum) throws SQLException {
        return new PlanTotals(r.getLong("total_tx"), r.getBigDecimal("total_amount"));
    }
}
