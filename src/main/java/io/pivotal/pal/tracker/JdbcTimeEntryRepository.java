package io.pivotal.pal.tracker;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;

public class JdbcTimeEntryRepository implements TimeEntryRepository {
    private JdbcTemplate jdbcTemplate;

    public JdbcTimeEntryRepository(DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public TimeEntry create(TimeEntry inTimeEntry) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        String sql = "INSERT INTO time_entries VALUES (null, ?, ?, ?, ?)";
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setLong(1, inTimeEntry.getProjectId());
            ps.setLong(2, inTimeEntry.getUserId());
            ps.setDate(3, Date.valueOf(inTimeEntry.getDate()));
            ps.setInt(4, inTimeEntry.getHours());
            return ps;
        }, keyHolder);

        inTimeEntry.setId(keyHolder.getKey().longValue());
        return inTimeEntry;
    }

    @Override
    public TimeEntry find(long id) {
        String sql = "SELECT * FROM time_entries WHERE id = ?";
        List<TimeEntry> result = jdbcTemplate.query(sql, timeEntryRowMapper(), id);
        if (result.size() == 0) { return null; }
        return result.get(0);
    }

    @Override
    public List<TimeEntry> list() {
        String sql = "SELECT * FROM time_entries";
        return jdbcTemplate.query(sql, timeEntryRowMapper());
    }

    @Override
    public TimeEntry update(long id, TimeEntry inTimeEntry) {
        String sql = "UPDATE time_entries SET project_id = ?, user_id = ?, date = ?, hours = ? WHERE id = ?";
        jdbcTemplate.update(sql,
                inTimeEntry.getProjectId(),
                inTimeEntry.getUserId(),
                inTimeEntry.getDate(),
                inTimeEntry.getHours(),
                id
        );
        inTimeEntry.setId(id);
        return inTimeEntry;
    }

    @Override
    public void delete(long id) {
        jdbcTemplate.update("DELETE FROM time_entries WHERE id = ?", id);
    }

    private RowMapper<TimeEntry> timeEntryRowMapper() {
        return new RowMapper<>() {
            @Override
            public TimeEntry mapRow(ResultSet rs, int rowNum) throws SQLException {
                TimeEntry selectedTimeEntry = new TimeEntry();
                selectedTimeEntry.setId(rs.getLong("id"));
                selectedTimeEntry.setProjectId(rs.getLong("project_id"));
                selectedTimeEntry.setUserId(rs.getLong("user_id"));
                selectedTimeEntry.setDate(rs.getDate("date").toLocalDate());
                selectedTimeEntry.setHours(rs.getInt("hours"));
                return selectedTimeEntry;
            }
        };
    }
}
