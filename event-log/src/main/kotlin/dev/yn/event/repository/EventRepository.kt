package dev.yn.event.repository

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.*
import dev.yn.event.domain.Event
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import dev.yn.util.date.*
import dev.yn.cassandra.CassandraConnector


class EventRepository(val cassandraConnector: CassandraConnector) {
    fun initialize() {
        cassandraConnector.session().execute(EventCQL.createKeyspace)
        cassandraConnector.session().execute(EventCQL.createTableStatement)
        cassandraConnector.session().execute(EventCQL.userIdIndexStatment)
        cassandraConnector.session().execute(EventCQL.eventTypeIndexStatment)
    }

    fun create(event: Event): Boolean {
        return cassandraConnector.session().execute(EventCQL.addStatement(event))
                .firstOrNull()
                ?.getBool(0)
                ?: false
    }

    fun getAll(resourceId: UUID): List<Event> {
        return cassandraConnector.session().execute(EventCQL.getAll(resourceId))
            .map(EventCQL.rowToEvent)
    }

    fun getEarliest(resourceId: UUID): Event? {
        return cassandraConnector.session().execute(EventCQL.getEasliest(resourceId))
                .firstOrNull()
                ?.let(EventCQL.rowToEvent)
    }

    fun getLatest(resourceId: UUID): Event? {
        return cassandraConnector.session().execute(EventCQL.getLatest(resourceId))
                .firstOrNull()
                ?.let(EventCQL.rowToEvent)
    }


    fun getAfterDate(resourceId: UUID, earliest: Date): List<Event> {
        return cassandraConnector.session().execute(EventCQL.getAfterDate(resourceId, earliest))
                .map(EventCQL.rowToEvent)
    }


    fun getOnDateRs(resourceId: UUID, date: String): ResultSet {
        return cassandraConnector.session().execute(EventCQL.getOnDate(resourceId, date))
    }

    fun getOnDate(resourceId: UUID, date: String): List<Event> {
        return cassandraConnector.session().execute(EventCQL.getOnDate(resourceId, date))
                .map(EventCQL.rowToEvent)
    }

    fun getBetweenDates(resourceId: UUID, earliest: Date, latest: Date): List<Event> {
        return cassandraConnector.session().execute(EventCQL.getBetweenDates(resourceId, earliest, latest))
                .map(EventCQL.rowToEvent)
    }

    fun get(resourceId: UUID, earliest: Date? = null, latest: Date? = null): List<Event> {
        return cassandraConnector.session().execute(EventCQL.getStatement(resourceId, earliest, latest))
                .map(EventCQL.rowToEvent)
    }

    fun getForUser(resourceId: UUID, userId: UUID, earliest: Date? = null, latest: Date? = null): List<Event> {
        return cassandraConnector.session().execute(EventCQL.getForUserStatement(resourceId, userId, earliest, latest))
                .map(EventCQL.rowToEvent)
    }
}

object EventCQL {
    val ResourceIdColumn = "resource_id"
    val DateColumn = "date"
    val EventTimeColumn = "event_time"
    val EventTypeColumn = "event_type"
    val BodyColumn = "body"
    val TableName = "events"
    val UserIdColumn = "user_id"
    val KeyspaceName = "event"

    val rowToEvent = { row: Row ->
        Event(row.getUUID(ResourceIdColumn),
                row.getTimestamp(EventTimeColumn),
                row.getString(EventTypeColumn),
                row.getUUID(UserIdColumn),
                row.getBytes(BodyColumn))
    }

    fun addStatement(event: Event): Insert {
        return QueryBuilder.insertInto(KeyspaceName, TableName)
                .value(ResourceIdColumn, event.resourceId)
                .value(DateColumn, dayFormat.format(event.eventTime))
                .value(EventTimeColumn, event.eventTime)
                .value(EventTypeColumn, event.eventType)
                .value(UserIdColumn, event.userId)
                .value(BodyColumn, event.body)
                .ifNotExists()
    }

    fun getAll(resourceId: UUID): Select {
        return QueryBuilder.select(ResourceIdColumn, EventTimeColumn, EventTypeColumn, UserIdColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .orderBy(QueryBuilder.asc(DateColumn), QueryBuilder.asc(EventTimeColumn))
    }

    fun getEasliest(resourceId: UUID): Select {
        return QueryBuilder.select(ResourceIdColumn, EventTimeColumn, EventTypeColumn, UserIdColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .orderBy(QueryBuilder.asc(DateColumn), QueryBuilder.asc(EventTimeColumn))
                .limit(1)
    }

    fun getLatest(resourceId: UUID): Select {
        return QueryBuilder.select(ResourceIdColumn, EventTimeColumn, EventTypeColumn, UserIdColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .orderBy(QueryBuilder.desc(DateColumn), QueryBuilder.desc(EventTimeColumn))
                .limit(1)
    }

    fun getOnDate(resourceId: UUID, date: String): Select {
        return QueryBuilder.select(ResourceIdColumn, EventTimeColumn, EventTypeColumn, UserIdColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .and(QueryBuilder.eq(DateColumn, date))
                .orderBy(QueryBuilder.asc(DateColumn), QueryBuilder.asc(EventTimeColumn))
    }

    fun getAfterDate(resourceId: UUID, earliest: Date): Select {
        return QueryBuilder.select(ResourceIdColumn, EventTimeColumn, EventTypeColumn, UserIdColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .and(QueryBuilder.gt(DateColumn, dayFormat.format(earliest)))
                .orderBy(QueryBuilder.asc(DateColumn), QueryBuilder.asc(EventTimeColumn))
    }

    fun getBetweenDates(resourceId: UUID, earliest: Date, latest: Date): Select {
        return QueryBuilder.select(ResourceIdColumn, EventTimeColumn, EventTypeColumn, UserIdColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .and(QueryBuilder.gt(DateColumn, dayFormat.format(earliest)))
                .and(QueryBuilder.lt(DateColumn, dayFormat.format(latest)))
                .orderBy(QueryBuilder.asc(DateColumn), QueryBuilder.asc(EventTimeColumn))
    }

    fun getStatement(resourceId: UUID, earliest: Date? = null, latest: Date? = null): Select {
        val earliestDate = earliest ?: Date.from((latest?.let { Instant.ofEpochMilli(it.time) } ?: Instant.now()).minus(1, ChronoUnit.DAYS))
        val latestDate = latest ?: Date.from(earliest ?.let { Instant.ofEpochMilli(it.time).plus(1, ChronoUnit.DAYS)} ?: Instant.now())

        return QueryBuilder.select(ResourceIdColumn, EventTimeColumn, EventTypeColumn, UserIdColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .and(QueryBuilder.`in`(DateColumn, constructDayList(earliestDate, latestDate)))
                .and(QueryBuilder.lte(EventTimeColumn, latestDate))
                .and(QueryBuilder.gte(EventTimeColumn, earliestDate))
                .orderBy(QueryBuilder.asc(DateColumn), QueryBuilder.asc(EventTimeColumn))
    }

    fun getForUserStatement(resourceId: UUID, userId: UUID, earliest: Date? = null, latest: Date? = null): Select.Where {
        val earliestDate = earliest ?: Date.from((latest?.let { Instant.ofEpochMilli(it.time) } ?: Instant.now()).minus(1, ChronoUnit.DAYS))
        val latestDate = latest ?: Date.from(earliest ?.let { Instant.ofEpochMilli(it.time).plus(1, ChronoUnit.DAYS)} ?: Instant.now())

        return QueryBuilder.select(ResourceIdColumn, EventTimeColumn, EventTypeColumn, UserIdColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .and(QueryBuilder.eq(DateColumn, dayFormat.format(latestDate)))
                .and(QueryBuilder.lte(EventTimeColumn, latestDate))
                .and(QueryBuilder.gte(EventTimeColumn, earliestDate))
                .and(QueryBuilder.eq(UserIdColumn, userId))
    }

    val createKeyspace = """
            CREATE KEYSPACE IF NOT EXISTS $KeyspaceName
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}
                AND DURABLE_WRITES = true;"""

    val createTableStatement = """
            CREATE TABLE IF NOT EXISTS $KeyspaceName.$TableName (
                $ResourceIdColumn uuid,
                $DateColumn text,
                $EventTimeColumn timestamp,
                $EventTypeColumn text,
                $UserIdColumn uuid,
                $BodyColumn blob,
                PRIMARY KEY ($ResourceIdColumn, $DateColumn, $EventTimeColumn)
            ) WITH CLUSTERING ORDER BY ($DateColumn asc, $EventTimeColumn asc);
        """

    val EventTypeIndexName = "${TableName}_${EventTypeColumn}_idx"
    val UserIdIndexName = "${TableName}_${UserIdColumn}_idx"
    val eventTypeIndexStatment = "CREATE INDEX IF NOT EXISTS ${EventTypeIndexName} ON $KeyspaceName.$TableName ($EventTypeColumn);"
    val userIdIndexStatment = "CREATE INDEX  IF NOT EXISTS ${UserIdIndexName} ON $KeyspaceName.$TableName ($UserIdColumn);"
    val dropEventTypeIndexStatment = "DROP INDEX IF EXISTS ${KeyspaceName}.${EventTypeIndexName};"
    val dropUserIdIndexStatment = "DROP INDEX IF EXISTS ${KeyspaceName}.${UserIdIndexName};"
    val dropTableStatement = "DROP TABLE IF EXISTS $KeyspaceName.$TableName;"
    val truncateTableStatement = "TRUNCATE  ${KeyspaceName}.${TableName};"

}