import groovy.lang.Binding
import groovy.sql.Sql
import nl.chess.intellix.UpgradeHelper
import nl.chess.intellix.core.Dimension
import nl.chess.intellix.core.ViewCreator;
import nl.chess.intellix.extract.*
import nl.chess.it.upgrademanager.UpgradeManager
import nl.chess.it.upgrademanager.jdbc.JDBCUpgradeManagerFactoryBean

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.mchange.v2.c3p0.ComboPooledDataSource


Logger LOG = LoggerFactory.getLogger(FileDump.class);

String.metaClass.nullify = {
	->
	(delegate.size() == 0) ? null : delegate
}

def dbserver = args[0]
def dbport = args[1]
def dbname = args[2]
def dbuser = args[3]
def dbpwd = args[4]
def outputdir = args[5]

def ds = new ComboPooledDataSource()
ds.with {
	driverClass = 'com.mysql.jdbc.Driver'
	jdbcUrl = 'jdbc:mysql://' + dbserver + ':' + dbport + '/' + dbname + '?sessionVariables=lc_time_names=nl_nl'
	user = dbuser
	password = dbpwd
	properties.setProperty('com.mchange.v2.c3p0.management.ManagementCoordinator', 'com.mchange.v2.c3p0.management.NullManagementCoordinator')
}

def views = ['v_opportunities', 'v_uren', 'v_taakbudget', 'v_taakbudget_uren']
Sql sql = Sql.newInstance(ds)
views.each { view ->
	def columns = []
	sql.eachRow("select column_name from information_schema.columns where table_name=$view") { columns << it.column_name }
	
	File file = new File(outputdir + '/' + view + '.csv')
	if (file.exists()) {
		file.delete()
	}
	file.withWriter  { out ->
		out.writeLine(columns.join('\t'))
		sql.eachRow("select * from $view".toString()) { row ->
			def foo = []
			columns.each { col ->
				foo << row.getAt(col)
			}
			out.writeLine(foo.join('\t'))
		}
	}
}