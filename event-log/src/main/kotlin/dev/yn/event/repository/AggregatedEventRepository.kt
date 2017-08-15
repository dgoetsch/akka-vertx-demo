package dev.yn.event.repository

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.Select
import dev.yn.event.domain.AggregateEvent
import dev.yn.util.date.dayFormat
import dev.yn.cassandra.CassandraConnector
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*

class AggregateEventRepository(val cassandraConnector: CassandraConnector) {
    fun initialize() {
        cassandraConnector.session().execute(AggregateEventCQL.createKeyspace)
        cassandraConnector.session().execute(AggregateEventCQL.createTableStatement)
        cassandraConnector.session().execute(AggregateEventCQL.eventTypesIndexStatement)
        cassandraConnector.session().execute(AggregateEventCQL.userIdIndexStatment)
    }

    fun createIfNotExists(event: AggregateEvent): Boolean {
        return cassandraConnector.session().execute(AggregateEventCQL.createIfNotExistsStatement(event))
            .map { it.getBool(0) }
            .firstOrNull() ?: false
    }

    fun create(event: AggregateEvent): Boolean {
        return cassandraConnector.session().execute(AggregateEventCQL.createStatement(event))
                .map { it.getBool(0) }
                .firstOrNull() ?: false
    }

    fun getSince(resourceId: UUID, earliest: Date): List<AggregateEvent> {
        return cassandraConnector.session().execute(AggregateEventCQL.getAfterDate(resourceId, earliest))
            .map(AggregateEventCQL.rowToEvent)
    }

    fun getLatest(resourceId: UUID): AggregateEvent? {
        return cassandraConnector.session().execute(AggregateEventCQL.getLatest(resourceId))
            .map(AggregateEventCQL.rowToEvent)
            .firstOrNull()
    }

    fun get(resourceId: UUID, earliest: Date? = null, latest: Date? = null): List<AggregateEvent> {
        return cassandraConnector.session().execute(AggregateEventCQL.getStatement(resourceId, earliest, latest))
            .map(AggregateEventCQL.rowToEvent)
    }

    fun getForUser(resourceId: UUID, userId: UUID, earliest: Date? = null, latest: Date? = null): List<AggregateEvent> {
        return cassandraConnector.session().execute(AggregateEventCQL.getForUserStatement(resourceId, userId, earliest, latest))
                .map(AggregateEventCQL.rowToEvent)
    }
}

object AggregateEventCQL {
    val ResourceIdColumn = "resource_id"
    val DateColumn = "date"
    val EventTypesColumn = "event_types"
    val UserIdsColumn = "user_ids"
    val BodyColumn = "body"
    val TableName = "aggregate_event"
    val KeyspaceName = "event"

    val rowToEvent = { row: Row ->
        AggregateEvent(row.getUUID(ResourceIdColumn),
                dayFormat.parse(row.getString(DateColumn)),
                row.getList(EventTypesColumn, String::class.java),
                row.getList(UserIdsColumn, UUID::class.java),
                row.getBytes(BodyColumn))
    }

    fun createIfNotExistsStatement(event: AggregateEvent): Insert {
        return QueryBuilder.insertInto(KeyspaceName, TableName)
                .value(ResourceIdColumn, event.resourceId)
                .value(DateColumn, dayFormat.format(event.date))
                .value(EventTypesColumn, event.eventTypes)
                .value(UserIdsColumn, event.userIds)
                .value(BodyColumn, event.body)
                .ifNotExists()
    }

    fun createStatement(event: AggregateEvent): Insert {
        return QueryBuilder.insertInto(KeyspaceName, TableName)
                .value(ResourceIdColumn, event.resourceId)
                .value(DateColumn, dayFormat.format(event.date))
                .value(EventTypesColumn, event.eventTypes)
                .value(UserIdsColumn, event.userIds)
                .value(BodyColumn, event.body)
    }

    fun getAfterDate(resourceId: UUID, earliest: Date): Select {
        return QueryBuilder.select(ResourceIdColumn, DateColumn, EventTypesColumn, UserIdsColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .and(QueryBuilder.gte(DateColumn, dayFormat.format(earliest)))
                .orderBy(QueryBuilder.asc(DateColumn))
    }

    fun getLatest(resourceId: UUID): Select {
        return QueryBuilder.select(ResourceIdColumn, DateColumn, EventTypesColumn, UserIdsColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .orderBy(QueryBuilder.desc(DateColumn))
                .limit(1)
    }

    fun getStatement(resourceId: UUID, earliest: Date? = null, latest: Date? = null): Select {
        val earliestDate = earliest ?: Date.from((latest?.let { Instant.ofEpochMilli(it.time) } ?: Instant.now()).minus(1, ChronoUnit.DAYS))
        val latestDate = latest ?: Date.from(earliest ?.let { Instant.ofEpochMilli(it.time).plus(1, ChronoUnit.DAYS)} ?: Instant.now())

        return QueryBuilder.select(ResourceIdColumn, DateColumn, EventTypesColumn, UserIdsColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .and(QueryBuilder.lte(DateColumn, dayFormat.format(latestDate)))
                .and(QueryBuilder.gte(DateColumn, dayFormat.format(earliestDate)))
                .orderBy(QueryBuilder.asc(DateColumn))
    }

    fun getForUserStatement(resourceId: UUID, userId: UUID, earliest: Date? = null, latest: Date? = null): Select.Where {
        val earliestDate = earliest ?: Date.from((latest?.let { Instant.ofEpochMilli(it.time) } ?: Instant.now()).minus(1, ChronoUnit.DAYS))
        val latestDate = latest ?: Date.from(earliest ?.let { Instant.ofEpochMilli(it.time).plus(1, ChronoUnit.DAYS)} ?: Instant.now())

        return QueryBuilder.select(ResourceIdColumn, DateColumn, EventTypesColumn, UserIdsColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .and(QueryBuilder.lte(DateColumn, dayFormat.format(latestDate)))
                .and(QueryBuilder.gte(DateColumn, dayFormat.format(earliestDate)))
                .and(QueryBuilder.contains(UserIdsColumn, userId))
    }

    val createKeyspace = """
            CREATE KEYSPACE IF NOT EXISTS $KeyspaceName
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}
                AND DURABLE_WRITES = true;"""

    val createTableStatement = """
            CREATE TABLE IF NOT EXISTS $KeyspaceName.$TableName (
                $ResourceIdColumn uuid,
                $DateColumn text,
                $EventTypesColumn list<text>,
                $UserIdsColumn list<uuid>,
                $BodyColumn blob,
                PRIMARY KEY ($ResourceIdColumn, $DateColumn)
            ) WITH CLUSTERING ORDER BY ($DateColumn asc);
        """

    val EventTypeIndexName = "${TableName}_${EventTypesColumn}_idx"
    val UserIdIndexName = "${TableName}_${UserIdsColumn}_idx"
    val eventTypesIndexStatement = "CREATE INDEX IF NOT EXISTS $EventTypeIndexName ON $KeyspaceName.$TableName ($EventTypesColumn);"
    val userIdIndexStatment = "CREATE INDEX IF NOT EXISTS $UserIdIndexName ON $KeyspaceName.$TableName ($UserIdsColumn);"
    val dropEventTypeIndexStatment = "DROP INDEX IF EXISTS $KeyspaceName.$EventTypeIndexName;"
    val dropUserIdIndexStatment = "DROP INDEX IF EXISTS $KeyspaceName.$UserIdIndexName;"
    val dropTableStatement = "DROP TABLE IF EXISTS $KeyspaceName.$TableName;"
}