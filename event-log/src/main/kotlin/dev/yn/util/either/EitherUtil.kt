package dev.yn.util.either

import dev.yn.entity.domain.EntityError
import org.funktionale.either.Either
import org.funktionale.either.flatMap

data class AggregateResult<E, T>(val results: List<T>, val errors: List<E>)

fun <E, T> List<Either<E, T>>.aggregateResult(): AggregateResult<E, T> {
    return this.fold(AggregateResult<E, T>(emptyList(), emptyList())) { aggregate, either ->
        either.fold({ error -> aggregate.copy(errors = aggregate.errors + error) }, { result -> aggregate.copy(results = aggregate.results + result) })
    }
}

fun <T> Either<EntityError, T?>.nonNull(): Either<EntityError, T> {
    return this.right().flatMap {
        it?.let { Either.Right<EntityError, T>(it) } ?:
                Either.Left<EntityError, T>(EntityError.MissingField(""))
    }
}