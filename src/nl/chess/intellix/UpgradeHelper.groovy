package nl.chess.intellix

import groovy.sql.Sql
import nl.chess.it.upgrademanager.ComponentInfo
import nl.chess.it.upgrademanager.UpgradeResult
import nl.chess.it.upgrademanager.Upgradeable
import nl.chess.it.upgrademanager.exceptions.UpgradeException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource

class UpgradeHelper implements Upgradeable {
    private static final Logger LOG = LoggerFactory.getLogger(UpgradeHelper.class)

    DataSource dataSource;

    @Override
    public ComponentInfo getComponentInfo() {
        return new ComponentInfo("DWH Tables", 1)
    }

    @Override
    public UpgradeResult upgrade(int fromVersion) throws UpgradeException {
        UpgradeResult result = null;
        def sql = Sql.newInstance(dataSource)

        switch (fromVersion) {
            case 0:
                LOG.info('Upgrading from version 0 to 1.')
                sql.execute """
				create table di_task (
		    		ID       		bigint not null auto_increment,
					type1hash		binary(32),
					lastUpdated		datetime not null,
		
		    		task_id		 bigint not null,
		    		task_name    varchar(255) not null,
		    		task_state   varchar(35) not null,
		    		sprint_id    bigint not null,
		    		sprint_name  varchar(255) not null,
		    		project_id   bigint not null,
		    		project_name varchar(255) not null,

					constraint primary key (ID),
					index (type1hash)
				) engine=InnoDB
			"""

                sql.execute """
				create table st_task (
		    		ID       		bigint,
					rowid			bigint not null auto_increment,
					action			char(1),
					type1hash		binary(32),

                    task_id      bigint not null,
                    task_name    varchar(255) not null,
                    task_state   varchar(35) not null,
                    sprint_id    bigint not null,
                    sprint_name  varchar(255) not null,
                    project_id   bigint not null,
                    project_name varchar(255) not null,

					unique index (ID),
					unique index (rowid),
					index (action),
					index (type1hash)
				) engine=InnoDB
			"""


            sql.execute """
				create table di_story (
                    ID              bigint not null auto_increment,
                    type1hash       binary(32),
                    lastUpdated     datetime not null,

                    story_id        bigint not null,
                    story_name      varchar(255) not null,
                    story_state     varchar(35) not null,
                    story_points    int not null,
                    story_value     int not null,
                    story_points_present boolean not null,
                    story_value_present boolean not null,

                    constraint primary key (ID),
                    index (type1hash)
				) engine=InnoDB
			"""

            sql.execute """
				create table st_story (
		    		ID       		bigint,
					rowid			bigint not null auto_increment,
					action			char(1),
					type1hash		binary(32),

                    story_id        bigint not null,
                    story_name      varchar(255) not null,
                    story_state     varchar(35) not null,
                    story_points    int not null,
                    story_value     int not null,
                    story_points_present boolean not null,
                    story_value_present boolean not null,

					unique index (ID),
					unique index (rowid),
					index (action),
					index (type1hash)
				) engine=InnoDB
			"""

            sql.execute """
				create table di_hourentry (
                    ID              bigint not null auto_increment,
                    type1hash       binary(32),
                    lastUpdated     datetime not null,

                    story_id                  bigint not null,
                    task_id                   bigint not null,
                    original_estimate_minutes int not null,
                    original_estimate_hours   decimal(12, 1) not null,
                    effort_left_minutes       int not null,
                    effort_left_hours         decimal(12, 1) not null,
                    effort_spent_minutes      int not null,
                    effort_spent_hours        decimal(12, 1) not null,

                    constraint primary key (ID),
                    index (type1hash),
                    foreign key (story_id) references di_story(ID),
                    foreign key (task_id) references di_task(ID)
				) engine=InnoDB
			"""

            sql.execute """
				create table st_hourentry (
		    		ID       		bigint,
					rowid			bigint not null auto_increment,
					action			char(1),
					type1hash		binary(32),

                    story_id                  bigint not null,
                    task_id                   bigint not null,
                    original_estimate_minutes int not null,
                    original_estimate_hours   decimal(12, 1) not null,
                    effort_left_minutes       int not null,
                    effort_left_hours         decimal(12, 1) not null,
                    effort_spent_minutes      int not null,
                    effort_spent_hours        decimal(12, 1) not null,

					unique index (ID),
					unique index (rowid),
					index (action),
					index (type1hash)
				) engine=InnoDB
			"""

                result = new UpgradeResult(1, "Initial schema created.");
                break;
            default:
                throw new UpgradeException("Cannot upgrade from unknown version " + fromVersion + ".")
        }

        return result
    }
}
