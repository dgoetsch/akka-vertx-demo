package dev.yn.cassandra

import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.Delete
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.Select
import org.funktionale.either.Either
import org.funktionale.tries.Try
import java.util.*

/*
This file is an example on how to use the cassandra database.  This may be used as a reference
when actually implementing cassandra tables
 */
data class Paging(val offset: Int, val limit: Int)

sealed class DatabaseError {
    class Conflict(val id: UUID, val type: String): DatabaseError()
    class NotFound(val id: UUID, val type: String): DatabaseError() {
        override fun equals(other: Any?): Boolean{
            if (this === other) return true
            if (other?.javaClass != javaClass) return false

            other as NotFound

            if (id != other.id) return false
            if (type != other.type) return false

            return true
        }
    }
    object UnspecifiedError: DatabaseError()
    class UnhandledException(val error: Throwable): DatabaseError()
    class AggregateError(val errors: List<DatabaseError>): DatabaseError()

    override fun equals(other: Any?): Boolean =
            when {
                other is Conflict && this is Conflict && this.id == other.id && this.type == other.type -> true
                other is NotFound && this is NotFound && this.id == other.id && this.type == other.type -> true
                other is UnspecifiedError && this is UnspecifiedError -> true
                other is UnhandledException && this is UnhandledException && this.error == other.error -> true
                other is AggregateError && this is AggregateError && this.errors == other.errors -> true
                else -> false
            }
}

interface NamespaceIdReferenceTableRow {
    val namespaceId: UUID
    val id: UUID
    val referenceId: UUID
    val updateTime: Long
}

abstract class NamespaceIdReferenceTable<RowType: NamespaceIdReferenceTableRow>(
        val NamespaceIdColumn: String,
        val IdColumn: String,
        val ReferenceColumn: String,
        val UpdatedTimeColumn: String,
        val TableName: String,
        val transformRow: Row.() -> RowType
) {
    fun get(relationship: RowType): (Session) -> org.funktionale.either.Either<DatabaseError, RowType> = {
        it.execute(getStatement(relationship))
                .all().firstOrNull()?.let {
            Either.Right<DatabaseError, RowType>(it.transformRow())
        } ?: Either.Left<DatabaseError, RowType>(DatabaseError.NotFound(relationship.id, TableName))
    }

    fun getMany(namespaceId: UUID, id: UUID, paging: Paging? = null): (Session) -> Either<DatabaseError, List<RowType>> = { session ->
        Try {
            val statement = getManyStatement(
                    namespaceId = namespaceId,
                    id = id)
            val result = paging?.let {
                CassandraPaging.fetchRowsWithPage(
                        statement = statement,
                        offset = paging.offset,
                        limit = paging.limit,
                        session = session)
            } ?: session.execute(statement)
            result.map { it.transformRow() }
        }.map { result ->
            result
        }.toDisjunction().toEither().left().map {
            DatabaseError.UnhandledException(it)
        }

    }

    fun getManyByMany(namespaceId: UUID, ids: List<UUID>): (Session) -> Either<DatabaseError, Map<UUID, List<RowType>>> = {
        Try {
            it.execute(getManyByManyStatement(namespaceId = namespaceId, ids = ids))
                    .all()
                    .map { it.transformRow() }
                    .groupBy { it.id }
        }.map { result ->
            result
        }.toDisjunction().toEither().left().map {
            DatabaseError.UnhandledException(it)
        }
    }

    private fun getStatement(relationship: RowType): Select.Where {
        return QueryBuilder.select(NamespaceIdColumn, IdColumn, ReferenceColumn, UpdatedTimeColumn)
                .from("entity_attribute", TableName)
                .where(QueryBuilder.eq(NamespaceIdColumn, relationship.namespaceId))
                .and(QueryBuilder.eq(IdColumn, relationship.id))
                .and(QueryBuilder.eq(ReferenceColumn, relationship.referenceId))
                .and(QueryBuilder.eq(UpdatedTimeColumn, relationship.updateTime))
    }

    private fun getManyStatement(namespaceId: UUID, id: UUID): Select.Where {
        val query = QueryBuilder.select(NamespaceIdColumn, IdColumn, ReferenceColumn, UpdatedTimeColumn)
                .from("entity_attribute", TableName)
                .where(QueryBuilder.eq(NamespaceIdColumn, namespaceId))
                .and(QueryBuilder.eq(IdColumn, id))
        return query
    }

    private fun getManyByManyStatement(namespaceId: UUID, ids: List<UUID>): Select.Where =
            QueryBuilder.select(NamespaceIdColumn, IdColumn, ReferenceColumn, UpdatedTimeColumn)
                    .from("entity_attribute", TableName)
                    .where(QueryBuilder.eq(NamespaceIdColumn, namespaceId))
                    .and(QueryBuilder.`in`(IdColumn, ids))

    fun removeByIdStatement(namespaceId: UUID, id: UUID): Delete.Where {
        return QueryBuilder.delete().from("entity_attribute", TableName)
                .where(QueryBuilder.eq(NamespaceIdColumn, namespaceId))
                .and(QueryBuilder.eq(IdColumn, id))
    }

    fun removeStatement(relationship: RowType): Delete.Conditions {
        return QueryBuilder.delete().from("entity_attribute", TableName)
                .where(QueryBuilder.eq(NamespaceIdColumn, relationship.namespaceId))
                .and(QueryBuilder.eq(IdColumn, relationship.id))
                .and(QueryBuilder.eq(ReferenceColumn, relationship.referenceId))
                .onlyIf(QueryBuilder.eq(UpdatedTimeColumn, relationship.updateTime))
    }

    fun addStatement(relationship: RowType): Insert {
        return QueryBuilder.insertInto("entity_attribute", TableName)
                .value(NamespaceIdColumn, relationship.namespaceId)
                .value(IdColumn, relationship.id)
                .value(ReferenceColumn, relationship.referenceId)
                .value(UpdatedTimeColumn, relationship.updateTime)
                .ifNotExists()
    }

    fun createTableStatement() = """
            CREATE TABLE IF NOT EXISTS entity_attribute.$TableName (
                $NamespaceIdColumn uuid,
                $IdColumn uuid,
                $ReferenceColumn uuid,
                $UpdatedTimeColumn bigint,
                PRIMARY KEY ($NamespaceIdColumn, $IdColumn, $ReferenceColumn));
        """
}