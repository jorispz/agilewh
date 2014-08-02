package nl.chess.intellix.extract

import groovy.sql.Sql
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class StoryExtractor extends AgileFantExtractor {
    Logger LOG = LoggerFactory.getLogger(StoryExtractor.class)

    void run() {
        Sql sourceSql = Sql.newInstance(sourceDS)
        Sql targetSql = Sql.newInstance(targetDS)

        LOG.info('Creating temporary table for stories.')
        targetSql.execute("drop table if exists ex_story")
        targetSql.execute('''
				create table ex_story (
                    id                  bigint,
                    name                varchar(255),
                    state_id            int,
                    state               varchar(32),
                    has_points           boolean,
                    has_value            boolean,
                    story_points         int,
                    story_value          int,
                    backlog_id          int,
                    parent_id           int,
                    parent_name         varchar(255),
                    product_id          int,
                    product_name        varchar(255),
                    project_id          int,
                    project_name        varchar(255),
                    sprint_id           int,
                    sprint_name         varchar(255)
				) engine=InnoDB
		''')

        LOG.info('Extracting stories from AgileFant.')
        targetSql.execute("""
          INSERT INTO ex_story
          (id, name, state_id, state, has_points, has_value, story_points, story_value, backlog_id, parent_id, parent_name, product_id, product_name, project_id, project_name, sprint_id, sprint_name)
          VALUES
          (-1, 'None', -1, 'Unknown', 0, 0, 0, 0, null, null, null, null, null, null, null, null, null)""")
        def tasks = sourceSql.rows("""
          select * from stories """)

        targetSql.withTransaction {
            targetSql.withBatch(50, 'insert into ex_story (id, name, state_id, has_points, has_value, story_points, story_value, backlog_id, parent_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?)') { stmt ->
                tasks.each {
                    boolean hasPoints = (it.storyPoints != null);
                    boolean hasValue = (it.storyValue != null);
                    int points = (hasPoints ? it.storyPoints : 0);
                    int value = (hasValue ? it.storyValue : 0);
                    stmt.addBatch([it.id, it.name, it.state, hasPoints, hasValue, points, value, it.backlog_id, it.parent_id])
                }
            }
        }

        targetSql.execute("""
          update ex_story st, ex_sprint sp
          set st.sprint_id = st.backlog_id, st.sprint_name = sp.name
          where st.backlog_id = sp.id""")
        targetSql.execute("""
          update ex_story st, ex_project pr
          set st.project_id = st.backlog_id, st.project_name = pr.name
          where st.backlog_id = pr.id""")
        targetSql.execute("""
          update ex_story st, ex_product pr
          set st.product_id = st.backlog_id, st.product_name = pr.name
          where st.backlog_id = pr.id""")

        targetSql.execute("""
          update ex_story st, ex_sprint sp, ex_project pr
          set st.project_id = pr.id, st.project_name = pr.name
          where sp.id = st.sprint_id
          and pr.id = sp.project_id""")

        targetSql.execute("""
          update ex_story st, ex_project pro, ex_product prod
          set st.product_id = prod.id, st.product_name = prod.name
          where st.project_id = pro.id
          and prod.id = pro.product_id""")

        targetSql.execute("""
          update ex_story st
          set st.project_id = -1, st.project_name = 'Not assigned'
          where st.project_id is null""")

        targetSql.execute("""
          update ex_story st
          set st.sprint_id = -1, st.sprint_name = 'Not assigned'
          where st.sprint_id is null""")

        targetSql.execute("""
          update ex_story st1, ex_story st2
          set st1.parent_name = st2.name
          where st2.id = st1.parent_id
          and st1.parent_id is not null""")

        targetSql.execute("""
        update ex_story story
        set story.state_id = -1 where story.state_id not in (select distinct id from ex_state)""")


        targetSql.execute("""
        update ex_story story, ex_state state
        set story.state = state.name where story.state_id = state.id""")
    }

}
