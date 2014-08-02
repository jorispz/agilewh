package nl.chess.intellix.extract

import groovy.sql.Sql
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class BacklogExtractor extends AgileFantExtractor {
    Logger LOG = LoggerFactory.getLogger(BacklogExtractor.class)

    void run() {
        Sql sourceSql = Sql.newInstance(sourceDS)
        Sql targetSql = Sql.newInstance(targetDS)

        LOG.info('Creating temporary table for backlogs.')
        targetSql.execute("drop table if exists ex_sprint")
        targetSql.execute("drop table if exists ex_project")
        targetSql.execute("drop table if exists ex_product")
        targetSql.execute('''
				create table ex_product (
				    id                  int primary  key,
                    name                varchar(255)
				) engine=InnoDB
		''')
        targetSql.execute('''
				create table ex_project (
				    id                  int primary key,
                    name                varchar(255),
                    product_id           int,

                    foreign key (product_id) references ex_product(id)
				) engine=InnoDB
		''')
        targetSql.execute('''
				create table ex_sprint (
				    id                  int primary key,
                    name                varchar(255),
                    project_id           int,
                    foreign key (project_id) references ex_project(id)
				) engine=InnoDB
		''')

        LOG.info('Extracting products from AgileFant.')
        def tasks = sourceSql.rows("""
          select * from backlogs where backlogtype='Product' """)

        targetSql.execute("""insert into ex_product(id, name) values (-1, 'None')""")

        targetSql.withTransaction {
            targetSql.withBatch(50, 'insert into ex_product (id, name) values (?, ?)') { stmt ->
                tasks.each {
                    stmt.addBatch([it.id, it.name])
                }
            }
        }

        LOG.info('Extracting projects from AgileFant.')
        tasks = sourceSql.rows("""
          select * from backlogs where backlogtype='Project' """)

        targetSql.execute("""insert into ex_project(id, name, product_id) values (-1, 'None', -1)""")
        targetSql.withTransaction {
            targetSql.withBatch(50, 'insert into ex_project (id, name, product_id) values (?, ?, ?)') { stmt ->
                tasks.each {
                    stmt.addBatch([it.id, it.name, it.parent_id])
                }
            }
        }

        LOG.info('Extracting sprints from AgileFant.')
        tasks = sourceSql.rows("""
          select * from backlogs where backlogtype='Iteration'""")

        targetSql.execute("""insert into ex_sprint(id, name, project_id) values (-1, 'None', -1)""")
        targetSql.withTransaction {
            targetSql.withBatch(50, 'insert into ex_sprint (id, name, project_id) values (?, ?, ?)') { stmt ->
                tasks.each {
                    stmt.addBatch([it.id, it.name, it.parent_id])
                }
            }
        }


    }

}
