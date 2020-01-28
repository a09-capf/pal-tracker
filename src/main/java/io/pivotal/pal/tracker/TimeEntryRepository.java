package io.pivotal.pal.tracker;

import java.util.List;

public interface TimeEntryRepository {
    TimeEntry create(TimeEntry inTimeEntry);

    TimeEntry find(long id);

    List<TimeEntry> list();

    TimeEntry update(long id, TimeEntry newTimeEntry);

    void delete(long id);
}
