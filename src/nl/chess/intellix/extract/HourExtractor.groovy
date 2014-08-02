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
                    entry_id        bigint,
                    entry_fullname  varchar(255),
                    minutes_spent   bigint,
                    story_id        int,
                    task_id         int
				) engine=InnoDB
		''')

        LOG.info('Extracting tasks from AgileFant.')
        def tasks = sourceSql.rows("""
          select h.id, u.fullname, sum(h.minutesSpent) as minutesSpent, h.story_id, h.task_id
          from hourentries h
          left outer join users u on h.user_id = u.id
          group by h.id, u.fullname, h.story_id, h.task_id""")

        targetSql.withTransaction {
            targetSql.withBatch(50, 'insert into ex_hourentry (entry_id, entry_fullname, minutes_spent, story_id, task_id) values (?, ?, ?, ?, ?)') { stmt ->
                tasks.each {
                    stmt.addBatch([it.id, it.fullname, it.minutesSpent, it.story_id, it.task_id])
                }
            }
        }

        targetSql.execute("""
          update ex_hourentry h, ex_task t
          set h.story_id = t.story_id
          where h.task_id = t.task_id
          and h.task_id is not null""")

        targetSql.execute("""
          update ex_hourentry h
          set h.story_id = -1
          where h.story_id is null""")

        targetSql.execute("""
          update ex_hourentry h
          set h.task_id = -1
          where h.task_id is null""")

    }

}
