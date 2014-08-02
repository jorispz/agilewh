package nl.chess.intellix.core


import groovy.sql.Sql

import javax.sql.DataSource

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Dimension {
	private static final Logger LOG = LoggerFactory.getLogger(Dimension.class)

	String name
	Set shadowDimensions = []
	List<String> naturalKeyColumns
	String transformSQL
	Closure afterMatch = {}
	Closure afterLoad = {sql, rows ->}

	void transformAndLoad(DataSource ds) {
		LOG.info("Starting transform and load for dimension '$name'.")
		def sql = Sql.newInstance(ds)

		def stagingtable = "st_" + name
		def dimensiontable = "di_" + name

		def adminColumns = ['rowid', 'action', 'type1hash', 'ID']
		def allColumns = []
		sql.eachRow("select column_name from information_schema.columns where table_name=${stagingtable}") { allColumns << it.column_name }
		def dimensionColumns = allColumns - adminColumns
		def dataColumns = dimensionColumns - naturalKeyColumns

		LOG.info("Column list for ${name}:")
		LOG.info("Admin columns                : " + adminColumns.join(", "))
		LOG.info("Natural key (type 2) columns : " + naturalKeyColumns.join(", "))
		LOG.info("Data (type 1) columns        : " + dataColumns.join(", "))

		LOG.debug("Truncating $stagingtable.")
		sql.execute "truncate table ${stagingtable}".toString()

		LOG.debug("Transforming data.")
		transformSQL.tokenize(';').each { sql.execute it }

		LOG.debug("Matching natural keys between $stagingtable and $dimensiontable.")
		sql.execute """ update $stagingtable s, $dimensiontable d
								set s.ID = d.ID
								where ${ naturalKeyColumns.collect{ col -> 's.' + col + ' = d.' + col}.join(' and ') } """.toString()

		afterMatch(sql)

		LOG.debug("Calculating hash over data columns.")
		sql.execute """ update ${stagingtable}
								set type1hash = md5(concat(${dataColumns.join(", ")})) """.toString()

		LOG.debug("Finding rows in $stagingtable to update.")
		sql.execute """	update $stagingtable s, $dimensiontable d
								set s.action = 'U'
								where s.ID = d.ID and s.type1hash <> d.type1hash """.toString()

		LOG.debug("Finding rows in $stagingtable to insert.")
		sql.execute """	update $stagingtable s
								set s.action = 'I'
								where s.ID is null """.toString()

		LOG.debug("Inserting rows into $dimensiontable.")
		sql.execute """	insert into $dimensiontable ( type1hash, lastUpdated, ${dimensionColumns.join(', ')} )
								select	type1hash, now(), ${dimensionColumns.join(', ')} from $stagingtable s
								where s.action  = 'I' """.toString()

		shadowDimensions.each { name ->
			def shadowTable = 'di_' + name
			LOG.debug("Inserting rows into shadow dimension table $shadowTable.")
			def targetColumns = dimensionColumns.collect { name + '_' + it }
			sql.execute """	insert into $shadowTable ( type1hash, lastUpdated, ${targetColumns.join(', ')} )
								select	type1hash, now(), ${dimensionColumns.join(', ')} from $stagingtable s
								where s.action  = 'I' """.toString()
		}

		LOG.debug("Executing updates on $dimensiontable.")
		sql.execute """	update $stagingtable s, $dimensiontable d
								set d.type1hash = s.type1hash, d.lastUpdated = now(), ${ dataColumns.collect{ col -> 'd.' + col + ' = s.' + col}.join(', ') }
								where d.ID = s.ID and s.action  = 'U' """.toString()

		shadowDimensions.each { name ->
			def shadowTable = 'di_' + name
			LOG.debug("Updating rows in shadow dimension table $shadowTable.")
			sql.execute """	update $stagingtable s, $shadowTable d
								set d.type1hash = s.type1hash, d.lastUpdated = now(), ${ dataColumns.collect{ col -> 'd.' + name + '_' + col + ' = s.' + col}.join(', ') }
								where d.ID = s.ID and s.action  = 'U' """.toString()
		}

		LOG.debug("Updating surrogate keys in $stagingtable.")
		sql.execute """ update $stagingtable s, $dimensiontable d
								set s.ID = d.ID
								where ${ naturalKeyColumns.collect{ col -> 's.' + col + ' = d.' + col}.join(' and ') } """.toString()

		LOG.debug("Generating list of surrogate keys no longer present in $stagingtable.")
		def rows = sql.rows """ select d.ID from $dimensiontable d
								where 
									d.ID
								not in
									( select s.ID from $stagingtable s )""".toString()

		LOG.debug("Found ${rows.size()} keys.")
		LOG.debug("Triggering after-load closure.")
		afterLoad(sql, rows)

		sql.close()
		LOG.info("Finished transform and load for dimension '$name'.")
	}
}
