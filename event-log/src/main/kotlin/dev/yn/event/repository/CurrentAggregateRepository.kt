package dev.yn.event.repository

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.*
import dev.yn.event.domain.AggregateEvent
import dev.yn.event.domain.Event
import dev.yn.cassandra.CassandraConnector
import java.util.*

class CurrentAggregateEventRepository(val cassandraConnector: CassandraConnector) {
    fun initialize() {
        cassandraConnector.session().execute(CurrentAggregateEventCQL.createKeyspace)
        cassandraConnector.session().execute(CurrentAggregateEventCQL.createTableStatement)
        cassandraConnector.session().execute(CurrentAggregateEventCQL.eventTypeIndexStatment)
        cassandraConnector.session().execute(CurrentAggregateEventCQL.userIdIndexStatment)
    }


    fun delete(event: Event): Boolean {
        return cassandraConnector.session().execute(CurrentAggregateEventCQL.deleteStatement(event))
                .map { it.getBool(0) }
                .firstOrNull() ?: false
    }
    fun createIfNotExists(event: Event): Boolean {
        return cassandraConnector.session().execute(CurrentAggregateEventCQL.createIfNotExistsStatement(event))
            .map { it.getBool(0) }
            .firstOrNull() ?: false
    }

    fun create(event: Event): Boolean {
        return cassandraConnector.session().execute(CurrentAggregateEventCQL.createStatement(event))
                .map { it.getBool(0) }
                .firstOrNull() ?: false
    }

    fun update(event: Event, prevTime: Date): Boolean {
        return cassandraConnector.session().execute(CurrentAggregateEventCQL.updateStatement(event, prevTime))
            .map { it.getBool(0) }
            .firstOrNull() ?: false
    }

    fun createIfNotExists(event: AggregateEvent): Boolean {
        return cassandraConnector.session().execute(CurrentAggregateEventCQL.createIfNotExistsStatement(event))
                .map { it.getBool(0) }
                .firstOrNull() ?: false
    }

    fun create(event: AggregateEvent): Boolean {
        return cassandraConnector.session().execute(CurrentAggregateEventCQL.createStatement(event))
                .map { it.getBool(0) }
                .firstOrNull() ?: false
    }

    fun update(event: AggregateEvent, prevTime: Date): Boolean {
        return cassandraConnector.session().execute(CurrentAggregateEventCQL.updateStatement(event, prevTime))
                .map { it.getBool(0) }
                .firstOrNull() ?: false
    }

    fun updateAndResetEventTypeAndUserIdColumns(event: Event, prevTime: Date): Boolean {
        return cassandraConnector.session().execute(CurrentAggregateEventCQL.updateAndResetLists(event, prevTime))
            .map { it.getBool(0) }
            .firstOrNull() ?: false
    }

    fun updateAndResetEventTypeAndUserIdColumns(event: AggregateEvent, prevTime: Date): Boolean {
        return cassandraConnector.session().execute(CurrentAggregateEventCQL.updateAndResetLists(event, prevTime))
                .map { it.getBool(0) }
                .firstOrNull() ?: false
    }

    fun get(resourceId: UUID): AggregateEvent? {
        return cassandraConnector.session().execute(CurrentAggregateEventCQL.getStatement(resourceId))
            .map(CurrentAggregateEventCQL.rowToEvent)
            .firstOrNull()
    }
}

object CurrentAggregateEventCQL {
    val ResourceIdColumn = "resource_id"
    val EventTimeColumn = "event_time"
    val EventTypeColumn = "event_type"
    val BodyColumn = "body"
    val UserIdColumn = "user_id"
    val TableName = "current_aggregate"
    val KeyspaceName = "event"

    val rowToEvent = { row: Row ->
        AggregateEvent(row.getUUID(ResourceIdColumn),
                row.getTimestamp(EventTimeColumn),
                row.getList(EventTypeColumn, String::class.java),
                row.getList(UserIdColumn, UUID::class.java),
                row.getBytes(BodyColumn))
    }

    fun deleteStatement(event: Event): Delete.Conditions {
        return QueryBuilder.delete().all()
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, event.resourceId))
                .onlyIf(QueryBuilder.lte(EventTimeColumn, event.eventTime))
    }

    fun createIfNotExistsStatement(event: Event): Insert {
        return QueryBuilder.insertInto(KeyspaceName, TableName)
                .value(ResourceIdColumn, event.resourceId)
                .value(EventTimeColumn, event.eventTime)
                .value(EventTypeColumn, listOf(event.eventType))
                .value(UserIdColumn, listOf(event.userId))
                .value(BodyColumn, event.body)
                .ifNotExists()
    }

    fun createStatement(event: Event): Insert {
        return QueryBuilder.insertInto(KeyspaceName, TableName)
                .value(ResourceIdColumn, event.resourceId)
                .value(EventTimeColumn, event.eventTime)
                .value(EventTypeColumn, listOf(event.eventType))
                .value(UserIdColumn, listOf(event.userId))
                .value(BodyColumn, event.body)
                .ifNotExists()
    }

    fun createIfNotExistsStatement(event: AggregateEvent): Insert {
        return QueryBuilder.insertInto(KeyspaceName, TableName)
                .value(ResourceIdColumn, event.resourceId)
                .value(EventTimeColumn, event.date)
                .value(EventTypeColumn, event.eventTypes)
                .value(UserIdColumn, event.userIds)
                .value(BodyColumn, event.body)
                .ifNotExists()
    }

    fun createStatement(event: AggregateEvent): Insert {
        return QueryBuilder.insertInto(KeyspaceName, TableName)
                .value(ResourceIdColumn, event.resourceId)
                .value(EventTimeColumn, event.date)
                .value(EventTypeColumn, event.eventTypes)
                .value(UserIdColumn, event.userIds)
                .value(BodyColumn, event.body)
    }

    fun updateStatement(event: Event, prevTime: Date): Update.Conditions {
        return QueryBuilder.update(KeyspaceName, TableName)
                .with(QueryBuilder.set(EventTimeColumn, event.eventTime))
                .and(QueryBuilder.append(EventTypeColumn, event.eventType))
                .and(QueryBuilder.append(UserIdColumn, event.userId))
                .and(QueryBuilder.set(BodyColumn, event.body))
                .where(QueryBuilder.eq(ResourceIdColumn, event.resourceId))
                .onlyIf(QueryBuilder.eq(EventTimeColumn, prevTime))
    }

    fun updateStatement(event: AggregateEvent, prevTime: Date): Update.Conditions {
        return QueryBuilder.update(KeyspaceName, TableName)
                .with(QueryBuilder.set(EventTimeColumn, event.date))
                .and(QueryBuilder.appendAll(EventTypeColumn, event.eventTypes))
                .and(QueryBuilder.appendAll(UserIdColumn, event.userIds))
                .and(QueryBuilder.set(BodyColumn, event.body))
                .where(QueryBuilder.eq(ResourceIdColumn, event.resourceId))
                .onlyIf(QueryBuilder.eq(EventTimeColumn, prevTime))
    }

    fun updateAndResetLists(event: Event, prevTime: Date) : Update.Conditions {
        return QueryBuilder.update(KeyspaceName, TableName)
                .with(QueryBuilder.set(EventTimeColumn, event.eventTime))
                .and(QueryBuilder.set(EventTypeColumn, listOf(event.eventType)))
                .and(QueryBuilder.set(UserIdColumn, listOf(event.userId)))
                .and(QueryBuilder.set(BodyColumn, event.body))
                .where(QueryBuilder.eq(ResourceIdColumn, event.resourceId))
                .onlyIf(QueryBuilder.eq(EventTimeColumn, prevTime))
    }

    fun updateAndResetLists(event: AggregateEvent, prevTime: Date) : Update.Conditions {
        return QueryBuilder.update(KeyspaceName, TableName)
                .with(QueryBuilder.set(EventTimeColumn, event.date))
                .and(QueryBuilder.set(EventTypeColumn, event.eventTypes))
                .and(QueryBuilder.set(UserIdColumn, event.userIds))
                .and(QueryBuilder.set(BodyColumn, event.body))
                .where(QueryBuilder.eq(ResourceIdColumn, event.resourceId))
                .onlyIf(QueryBuilder.eq(EventTimeColumn, prevTime))
    }

    fun getStatement(resourceId: UUID): Select.Where {
        return QueryBuilder.select(ResourceIdColumn, EventTimeColumn, EventTypeColumn, UserIdColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
    }

    fun getForUserStatement(resourceId: UUID, userId: UUID): Select.Where {
        return QueryBuilder.select(ResourceIdColumn, EventTimeColumn, EventTypeColumn, UserIdColumn, BodyColumn)
                .from(KeyspaceName, TableName)
                .where(QueryBuilder.eq(ResourceIdColumn, resourceId))
                .and(QueryBuilder.contains(UserIdColumn, userId))
    }

    val createKeyspace = """
            CREATE KEYSPACE IF NOT EXISTS $KeyspaceName
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}
                AND DURABLE_WRITES = true;"""

    val createTableStatement = """
            CREATE TABLE IF NOT EXISTS $KeyspaceName.$TableName (
                $ResourceIdColumn uuid,
                $EventTimeColumn timestamp,
                $EventTypeColumn list<text>,
                $UserIdColumn list<uuid>,
                $BodyColumn blob,
                PRIMARY KEY ($ResourceIdColumn));
        """
    val EventTypeIndexName = "${TableName}_${EventTypeColumn}_idx"
    val UserIdIndexName = "${TableName}_${UserIdColumn}_idx"
    val eventTypeIndexStatment = "CREATE INDEX IF NOT EXISTS $EventTypeIndexName ON $KeyspaceName.$TableName ($EventTypeColumn);"
    val userIdIndexStatment = "CREATE INDEX IF NOT EXISTS $UserIdIndexName ON $KeyspaceName.$TableName ($UserIdColumn);"
    val dropEventTypeIndexStatment = "DROP INDEX IF EXISTS $KeyspaceName.$EventTypeIndexName;"
    val dropUserIdIndexStatment = "DROP INDEX IF EXISTS $KeyspaceName.$UserIdIndexName;"
    val dropTableStatement = "DROP TABLE IF EXISTS $KeyspaceName.$TableName;"
    val truncateTableStatement = "TRUNCATE $KeyspaceName.$TableName;"
}