package za.co.fnb.dcre.crw.data.repo;

import org.springframework.jdbc.core.RowMapper;
import za.co.fnb.dcre.crw.data.model.DueArrivalRow;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Reads the {@code client} column, which is {@link DueSql#CLIENT_EXPR} rather than a physical
 * column: the outbound client authority is resolved in the SQL so that the emission group's
 * column, the outbound file name and the per-client directory cannot come from different
 * sources (A-43, ruled 2026-08-08).
 */
public class DueArrivalRowMapper implements RowMapper<DueArrivalRow> {

    @Override
    public DueArrivalRow mapRow(ResultSet r, int rowNum) throws SQLException {
        return new DueArrivalRow(r.getObject("arrival_id", UUID.class), r.getString("client"),
                r.getString("msg_id"));
    }
}
