package nl.chess.intellix.core

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import groovy.sql.Sql;

class ViewCreator {
	private static final Logger LOG = LoggerFactory.getLogger(ViewCreator.class)

	DataSource dataSource
	String viewName
	String factTable
	Set ignoredColumns = [
		'ID',
		'type1hash',
		'lastUpdated'
	]

	void create() {
		Set tables = [factTable]
		def columns = []
		Set whereClauses = []

		Sql s = Sql.newInstance(dataSource)
		s.eachRow("select * from information_schema.table_constraints c, information_schema.key_column_usage k where c.table_name = ? and c.constraint_type = 'foreign key' and k.constraint_name = c.constraint_name", [factTable]) {
			tables << it.referenced_table_name
			whereClauses << it.table_name + '.' + it.column_name + ' = ' +  it.referenced_table_name + '.' + it.referenced_column_name
			ignoredColumns << it.column_name
			ignoredColumns << it.referenced_column_name
		}
		tables.each {table ->
			s.eachRow("select column_name from information_schema.columns where table_name=?", [table]) { row ->
				if (!(ignoredColumns.contains(row.column_name))) {
					columns << table + '.' + row.column_name
				}
			}
		}
		StringBuffer createViewStatement = new StringBuffer();
		createViewStatement.append('create or replace view ' + viewName + ' as select ')
		createViewStatement.append(columns.join(','))
		createViewStatement.append(' from ')
		createViewStatement.append(tables.join(','))
		createViewStatement.append(' where ')
		createViewStatement.append(whereClauses.join(' and '))

		s.execute(createViewStatement.toString())

		s.close()
	}
}
