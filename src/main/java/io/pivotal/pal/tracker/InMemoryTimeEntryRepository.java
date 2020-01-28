package io.pivotal.pal.tracker;

import org.springframework.stereotype.Repository;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class InMemoryTimeEntryRepository implements TimeEntryRepository {

    HashMap<Long, TimeEntry> repo = new HashMap<>();
    Long autoID = 0L;

    @Override
    public TimeEntry create(TimeEntry inTimeEntry) {
        Long id = ++autoID;
        TimeEntry newEntry = new TimeEntry(
                id,
                inTimeEntry.getProjectId(),
                inTimeEntry.getUserId(),
                inTimeEntry.getDate(),
                inTimeEntry.getHours()
        );
        repo.put(id, newEntry);
        return newEntry;
    }

    @Override
    public TimeEntry find(long id) {
        return repo.get(id);
    }

    @Override
    public List<TimeEntry> list() {
        Collection<TimeEntry> values = repo.values();
        ArrayList<TimeEntry> result = new ArrayList<>(values);
        return result;
    }

    @Override
    public TimeEntry update(long id, TimeEntry newTimeEntry) {
        TimeEntry existTimeEntry = repo.get(id);
        if (existTimeEntry == null) { return null; }
        existTimeEntry.setProjectId(newTimeEntry.getProjectId());
        existTimeEntry.setUserId(newTimeEntry.getUserId());
        existTimeEntry.setDate(newTimeEntry.getDate());
        existTimeEntry.setHours(newTimeEntry.getHours());
        return existTimeEntry;
    }

    @Override
    public void delete(long id) {
        repo.remove(id);
    }
}
