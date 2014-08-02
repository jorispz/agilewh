package nl.chess.intellix.extract

import groovy.sql.Sql
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class StateExtractor extends AgileFantExtractor {
    Logger LOG = LoggerFactory.getLogger(StateExtractor.class)

    void run() {

        Sql targetSql = Sql.newInstance(targetDS)

        LOG.info('Creating temporary table for state definitions.')
        targetSql.execute("drop table if exists ex_state")
        targetSql.execute('''
				create table ex_state (
				    id                  int primary  key,
                    name                varchar(255)
				) engine=InnoDB
		''')

        targetSql.execute("""
        insert into ex_state
        values (0, 'Not started'), (1, 'Started'), (2, 'Pending'), (3, 'Blocked'), (4, 'Ready'), (5, 'Done'), (6, 'Deferred'), (-1, 'Unknown')""")


    }

}
