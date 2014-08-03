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

        def tasksWithoutStory = targetSql.rows("""
            select distinct t.sprint_id, s.name as sprint_name, prj.id as project_id, prj.name as project_name, prd.id as product_id, prd.name as product_name from ex_task t
            inner join ex_sprint s
            on s.id = t.sprint_id
            inner join ex_project prj
            on s.project_id = prj.id
            inner join ex_product prd
            on prj.product_id = prd.id
            where t.story_id is null""")
        int id = -1;
        tasksWithoutStory.each {
            int projectID = it.project_id
            targetSql.execute("""
              INSERT INTO ex_story
              (id, name, state_id, state, has_points, has_value, story_points, story_value, backlog_id, parent_id, parent_name, product_id, product_name, project_id, project_name, sprint_id, sprint_name)
              VALUES
              ($id, 'None', -1, 'Unknown', 0, 0, 0, 0, null, null, null, $it.product_id, $it.product_name, $it.project_id, $it.project_name, $it.sprint_id, $it.sprint_name);
            """)
            targetSql.execute("""update ex_task set story_id = $id where story_id is null and sprint_id=$it.sprint_id""")
            id--
        }


    }

}
