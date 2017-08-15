package dev.yn.event.repository

import dev.yn.event.domain.AggregateEvent
import dev.yn.event.domain.Event
import dev.yn.util.string.toByteBuffer
import io.kotlintest.Spec
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.StringSpec
import dev.yn.cassandra.CassandraConnector
import java.time.Instant
import java.util.*


class CurrentAggregateRepositorySpec: StringSpec() {
    override protected fun interceptSpec(context: Spec, spec: () -> Unit) {
        val connector = CassandraConnector.singleton
        val currentAggregateRepo = CurrentAggregateEventRepository(connector)
        currentAggregateRepo.initialize()

        spec()


        CassandraConnector.singleton.session().execute(CurrentAggregateEventCQL.dropTableStatement)
    }

    init {
        val currentAggregateRepo = CurrentAggregateEventRepository(CassandraConnector.singleton)

        "should createIfNotExists a new incorporate event" {
            val event = Event(UUID.randomUUID(), Date(), "myetype", UUID.randomUUID(), "some random string".toByteBuffer())

            currentAggregateRepo.createIfNotExists(event) shouldBe true

            currentAggregateRepo.get(event.resourceId) shouldBe AggregateEvent(event.resourceId, event.eventTime, listOf(event.eventType), listOf(event.userId), event.body)
        }

        "should not insert a duplicate event" {
            val event = Event(UUID.randomUUID(), Date(), "myetype", UUID.randomUUID(), "some random string".toByteBuffer())

            currentAggregateRepo.createIfNotExists(event) shouldBe true

            val event2 = Event(event.resourceId, Date(), "myeType2", UUID.randomUUID(), "any string".toByteBuffer())
            currentAggregateRepo.createIfNotExists(event) shouldBe false
        }

        "should update an event if the timestmap is newer" {
            val event = Event(UUID.randomUUID(), Date(), "myetype", UUID.randomUUID(), "some random string".toByteBuffer())

            currentAggregateRepo.createIfNotExists(event) shouldBe true

            val update = Event(event.resourceId, Date(), "myetype2", UUID.randomUUID(), "the new incorporate".toByteBuffer())

            currentAggregateRepo.update(update, event.eventTime) shouldBe true

            currentAggregateRepo.get(event.resourceId) shouldBe AggregateEvent(event.resourceId, update.eventTime, listOf(event.eventType, update.eventType), listOf(event.userId, update.userId), update.body)
        }

        "should not update if the current state has a later timestamp than input" {
            val event = Event(UUID.randomUUID(), Date(), "myetype", UUID.randomUUID(), "some random string".toByteBuffer())

            currentAggregateRepo.createIfNotExists(event) shouldBe true

            val stale = Event(event.resourceId, Date.from(Instant.now().minusMillis(1000)), "staleType", UUID.randomUUID(), "stale".toByteBuffer())
            val update = Event(event.resourceId, Date(), "myetype2", UUID.randomUUID(), "the new incorporate".toByteBuffer())

            currentAggregateRepo.update(update, event.eventTime) shouldBe true
            currentAggregateRepo.update(stale, event.eventTime) shouldBe false

            currentAggregateRepo.get(event.resourceId) shouldBe AggregateEvent(event.resourceId, update.eventTime, listOf(event.eventType, update.eventType), listOf(event.userId, update.userId), update.body)
        }


        "should reset the state of eventType and userId of an event" {
            val event = Event(UUID.randomUUID(), Date(), "myetype", UUID.randomUUID(), "some random string".toByteBuffer())

            currentAggregateRepo.createIfNotExists(event) shouldBe true

            val update = Event(event.resourceId, Date(), "myetype2", UUID.randomUUID(), "the new incorporate".toByteBuffer())

            currentAggregateRepo.update(update, event.eventTime) shouldBe true

            val update2 = Event(event.resourceId, Date(), "myetype3", UUID.randomUUID(), "clear!".toByteBuffer())

            currentAggregateRepo.updateAndResetEventTypeAndUserIdColumns(update2, update.eventTime)
            currentAggregateRepo.get(event.resourceId) shouldBe AggregateEvent(event.resourceId, update2.eventTime, listOf(update2.eventType), listOf(update2.userId), update2.body)
        }

        "should not reset the state of eventType and userId of an event if the current state has a later timestamp than input" {
            val event = Event(UUID.randomUUID(), Date(), "myetype", UUID.randomUUID(), "some random string".toByteBuffer())

            currentAggregateRepo.createIfNotExists(event) shouldBe true

            val instant = Instant.now()
            val stale = Event(event.resourceId, Date.from(instant.minusMillis(1)), "staleType", UUID.randomUUID(), "stale".toByteBuffer())
            val update = Event(event.resourceId, Date.from(instant), "myetype2", UUID.randomUUID(), "the new incorporate".toByteBuffer())

            currentAggregateRepo.update(update, event.eventTime) shouldBe true

            val update2 = Event(event.resourceId, Date(), "myetype3", UUID.randomUUID(), "clear!".toByteBuffer())

            currentAggregateRepo.updateAndResetEventTypeAndUserIdColumns(update2, update.eventTime) shouldBe true
            currentAggregateRepo.updateAndResetEventTypeAndUserIdColumns(stale, update.eventTime) shouldBe false

            currentAggregateRepo.get(event.resourceId) shouldBe AggregateEvent(event.resourceId, update2.eventTime, listOf(update2.eventType), listOf(update2.userId), update2.body)
        }


    }

}
