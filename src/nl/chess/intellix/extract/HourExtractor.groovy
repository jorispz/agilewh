package nl.chess.intellix.extract

import groovy.sql.Sql
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class HourExtractor extends AgileFantExtractor {
    Logger LOG = LoggerFactory.getLogger(HourExtractor.class)

    void run() {
        Sql sourceSql = Sql.newInstance(sourceDS)
        Sql targetSql = Sql.newInstance(targetDS)

        LOG.info('Creating temporary table for tasks.')
        targetSql.execute("drop table if exists ex_hourentry")
        targetSql.execute('''
				create table ex_hourentry (
                    minutes_spent   bigint,
                    backlog_id      int,
                    story_id        int,
                    task_id         int,

                    index(story_id),
                    index(task_id)
				) engine=InnoDB
		''')

        LOG.info('Extracting tasks from AgileFant.')
        def tasks = sourceSql.rows("""
          select sum(h.minutesSpent) as minutesSpent, h.backlog_id, h.story_id, h.task_id
          from hourentries h
          group by h.backlog_id, h.story_id, h.task_id""")

        targetSql.withTransaction {
            targetSql.withBatch(50, 'insert into ex_hourentry (minutes_spent, backlog_id, story_id, task_id) values (?, ?, ?, ?)') { stmt ->
                tasks.each {
                    stmt.addBatch([it.minutesSpent, it.backlog_id, it.story_id, it.task_id])
                }
            }
        }

        targetSql.execute("""
          update ex_hourentry h, ex_task t
          set h.story_id = t.story_id
          where h.task_id = t.task_id
          and h.task_id is not null""")

        def hoursWithoutTask = targetSql.rows("""
            select distinct story_id, st.sprint_id
            from ex_hourentry h
            inner join ex_story st
            on h.story_id = st.id
            where task_id is null""")

        int minTaskID = targetSql.firstRow("select min(task_id) as task_id from ex_task")[0];

        hoursWithoutTask.each {
            minTaskID--;
            targetSql.execute("""
              INSERT INTO ex_task
              (task_id, task_name, has_original_estimate, has_effort_left, original_estimate, effort_left, state_id, state, sprint_id, story_id)
              VALUES
              ($minTaskID, 'None', 0, 0, 0, 0, -1, 'Unknown', $it.sprint_id, $it.story_id)""")
            targetSql.execute("""update ex_hourentry set task_id = $minTaskID where task_id is null and story_id=$it.story_id""")
        }

        targetSql.execute("""
            insert into ex_hourentry (minutes_spent, story_id, task_id)
            select 0 as minutes_spent, story_id, task_id from ex_task where ex_task.task_id not in (select task_id from ex_hourentry)
        """)







    }

}
