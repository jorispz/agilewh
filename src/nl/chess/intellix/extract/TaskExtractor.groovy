package nl.chess.intellix.extract

import groovy.sql.Sql
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TaskExtractor extends AgileFantExtractor {
    Logger LOG = LoggerFactory.getLogger(TaskExtractor.class)

    void run() {
        Sql sourceSql = Sql.newInstance(sourceDS)
        Sql targetSql = Sql.newInstance(targetDS)

        LOG.info('Creating temporary table for tasks.')
        targetSql.execute("drop table if exists ex_task")
        targetSql.execute('''
				create table ex_task (
                    task_id                 bigint,
                    task_name               varchar(255),
                    has_original_estimate   boolean not null,
                    has_effort_left         boolean not null,
                    original_estimate       int,
                    effort_left             int,
                    state_id                int,
                    state                   varchar(32),
                    sprint_id               int,
                    story_id                int
				) engine=InnoDB
		''')

        LOG.info('Extracting tasks from AgileFant.')
        def tasks = sourceSql.rows("""
          select * from tasks """)

        targetSql.execute("""
          INSERT INTO ex_task
            (task_id, task_name, has_original_estimate, has_effort_left, original_estimate, effort_left, state_id, state, sprint_id, story_id)
          VALUES
          (-1, 'None', 0, 0, 0, 0, -1, 'Unknown', -1, -1)""")

        targetSql.withTransaction {
            targetSql.withBatch(50, 'insert into ex_task (task_id, task_name, has_original_estimate, has_effort_left, original_estimate, effort_left, state_id, sprint_id, story_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?)') { stmt ->
                tasks.each {
                    boolean hasEstimate = (it.originalestimate != null);
                    boolean hasEffort = it.effortLeft != null;
                    int originalEstimate = (hasEstimate ? it.originalestimate : 0);
                    int effortLeft = (hasEffort ? it.effortleft : originalEstimate);

                    stmt.addBatch([it.id, it.name, hasEstimate, hasEffort, originalEstimate, effortLeft, it.state, it.iteration_id, it.story_id])
                }
            }
        }

        targetSql.execute("""
        update ex_task task
        set task.state_id = -1 where task.state_id not in (select distinct id from ex_state)""")


        targetSql.execute("""
        update ex_task task, ex_state state
        set task.state = state.name where task.state_id = state.id""")

        targetSql.execute("""
        update ex_task t, ex_story s
        set t.sprint_id = s.sprint_id
        where t.story_id = s.id
        and t.story_id is not null""")



    }

}
