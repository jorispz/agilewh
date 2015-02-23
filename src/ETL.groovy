import com.mchange.v2.c3p0.ComboPooledDataSource
import nl.chess.intellix.UpgradeHelper
import nl.chess.intellix.core.Dimension
import nl.chess.intellix.core.ViewCreator
import nl.chess.intellix.extract.*
import nl.chess.it.upgrademanager.UpgradeManager
import nl.chess.it.upgrademanager.jdbc.JDBCUpgradeManagerFactoryBean
import org.slf4j.Logger
import org.slf4j.LoggerFactory

Logger LOG = LoggerFactory.getLogger(ETL.class);

String.metaClass.nullify = {
	->
	(delegate.size() == 0) ? null : delegate
}

def sourceserver = args[0]
def sourceport = args[1]
def sourcename = args[2]
def sourceuser = args[3]
def sourcepwd = args[4]

def targetserver = args[5]
def targetport = args[6]
def targetname = args[7]
def targetuser = args[8]
def targetpwd = args[9]


def sourceDS = new ComboPooledDataSource()
sourceDS.with {
	driverClass = 'com.mysql.jdbc.Driver'
	jdbcUrl = 'jdbc:mysql://' + sourceserver + ':' + sourceport + '/' + sourcename + '?sessionVariables=lc_time_names=nl_nl'
	user = sourceuser
	password = sourcepwd
	properties.setProperty('com.mchange.v2.c3p0.management.ManagementCoordinator', 'com.mchange.v2.c3p0.management.NullManagementCoordinator')
}

def targetDS = new ComboPooledDataSource()
targetDS.with {
    driverClass = 'com.mysql.jdbc.Driver'
    jdbcUrl = 'jdbc:mysql://' + targetserver + ':' + targetport + '/' + targetname + '?sessionVariables=lc_time_names=nl_nl'
    user = targetuser
    password = targetpwd
    properties.setProperty('com.mchange.v2.c3p0.management.ManagementCoordinator', 'com.mchange.v2.c3p0.management.NullManagementCoordinator')
}

def umfb = new JDBCUpgradeManagerFactoryBean();
umfb.dataSource = targetDS
umfb.databaseType = "MYSQL41"
UpgradeManager um = umfb.object

um.install()
um.register(new UpgradeHelper(dataSource:targetDS))
um.performUpdates()

new StateExtractor(sourceDS:sourceDS, targetDS:targetDS).run();
new BacklogExtractor(sourceDS: sourceDS, targetDS: targetDS).run();
new StoryExtractor(sourceDS: sourceDS, targetDS: targetDS).run();
new TaskExtractor(sourceDS:sourceDS, targetDS: targetDS).run();
new HourExtractor(sourceDS:sourceDS, targetDS:targetDS).run();

Dimension task = new Dimension()
task.name = 'task'
task.naturalKeyColumns = ['task_ID']
task.transformSQL = """
	insert into st_task
	   (task_id, task_name, task_state)
	select
		t.task_id, t.task_name, t.state
	from ex_task t
"""
task.transformAndLoad(targetDS)

Dimension story = new Dimension()
story.name='story'
story.naturalKeyColumns = ['story_id']
story.transformSQL = """
    insert into st_story
        (story_id, story_name, story_state, story_points, story_value, story_points_present, story_value_present, sprint_id, sprint_name, project_id, project_name, label)
    select
        id, name, state, story_points, story_value, has_points, has_value, sprint_id, sprint_name, project_id, project_name, label
    from ex_story
"""
story.transformAndLoad(targetDS);

Dimension hour = new Dimension()
hour.name='hourentry'
hour.naturalKeyColumns = ['s_id', 't_id']
hour.transformSQL = """
    insert into st_hourentry
        (s_id, t_id, original_estimate_minutes, original_estimate_hours, effort_spent_minutes, effort_spent_hours, effort_left_minutes, effort_left_hours, date_year, date_month, date_day, user_full_name)
    select
         s.ID, t.ID, sum(et.original_estimate), sum(et.original_estimate)/60, sum(minutes_spent), sum(minutes_spent)/60, sum(et.effort_left), sum(et.effort_left)/60, date_year, date_month, date_day, user_full_name
    from ex_hourentry h
    inner join st_task t
    on h.task_id = t.task_id
    inner join st_story s
    on h.story_id = s.story_id
    inner join ex_task et
    on t.task_id = et.task_id
    group by s.ID, t.ID,    date_year,
    date_month,
    date_day,
    user_full_name"""
hour.transformAndLoad(targetDS)

LOG.info('Recreating reporting views.')
new ViewCreator(dataSource:targetDS, viewName: 'v_uren', factTable: 'di_hourentry').create()


LOG.info("Finished ETL process.")