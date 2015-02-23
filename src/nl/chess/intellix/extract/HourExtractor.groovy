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
                    date_year                 int,
                    date_month                int null,
                    date_day                  int null,
                    user_full_name            varchar(255),

                    index(story_id),
                    index(task_id)
				) engine=InnoDB
		''')

        LOG.info('Extracting tasks from AgileFant.')
        def tasks = sourceSql.rows("""
          SELECT
                SUM(h.minutesSpent) AS minutesSpent,
                h.backlog_id,
                h.story_id,
                h.task_id,
                year(h.date) as date_year,
                month(date) date_month,
                dayofmonth(date) as date_day,
                u.fullName
            FROM
                hourentries h
            INNER JOIN
                users u
            on h.user_id = u.id
            GROUP BY
                h.backlog_id,
                h.story_id,
                h.task_id,
                date_year,
                date_month,
                date_day,
                u.fullName""")

        targetSql.withTransaction {
            targetSql.withBatch(50, 'insert into ex_hourentry (minutes_spent, backlog_id, story_id, task_id, date_year, date_month, date_day, user_full_name) values (?, ?, ?, ?, ?, ?, ?, ?)') { stmt ->
                tasks.each {
                    stmt.addBatch([it.minutesSpent, it.backlog_id, it.story_id, it.task_id, it.date_year, it.date_month, it.date_day, it.fullName])
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


        // TODO: the 'is not null' clause in the query below deals with a corner case in the EQ project where hours were booked directly on a backlog.
        // This needs to be dealt with properly at some point, but for now we will ignore them (528 minutes in total)
        targetSql.execute("""
            insert into ex_hourentry (minutes_spent, story_id, task_id)

            select 0 as minutes_spent, story_id, task_id from ex_task where ex_task.task_id not in (select distinct task_id from ex_hourentry where task_id is not null)
        """)







    }

}
