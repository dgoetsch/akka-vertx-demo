package dev.yn.event.repository

import dev.yn.event.domain.Event
import dev.yn.event.repository.EventCQL
import io.kotlintest.Spec
import io.kotlintest.TestCaseContext
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.StringSpec
import dev.yn.cassandra.CassandraConnector
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import dev.yn.util.string.*
import org.funktionale.tries.Try

class EventRepositorySpec: StringSpec() {

    override protected fun interceptSpec(context: Spec, spec: () -> Unit) {
        val connector = CassandraConnector.singleton
        val eventRepo = EventRepository(connector)
        eventRepo.initialize()

        spec()

        CassandraConnector.singleton.session().execute(EventCQL.dropTableStatement)
    }

    override protected fun interceptTestCase(context: TestCaseContext, test: () -> Unit) {
        //before

        //execute test
        test()

        //after
    }

    init {
        val eventRepo = EventRepository(CassandraConnector.singleton)
        "should insert several events, and fetch them" {
            val event = Event(UUID.randomUUID(), Date.from(Instant.now()), "Type", UUID.randomUUID(), "body of event".toByteBuffer())
            eventRepo.create(event) shouldBe true
            eventRepo.get(event.resourceId) shouldBe listOf(event)
        }

        "should insert several rows, but only select rows within the date range" {
            val resourceId = UUID.randomUUID()
            val events = (0L..100L).map { Event(resourceId, Date.from(Instant.now().minus(it, ChronoUnit.HOURS)), "Event_Type", UUID.randomUUID(), "event body".toByteBuffer())}
            events.forEach { eventRepo.create(it) shouldBe true }
            eventRepo.get(resourceId, Date.from(Instant.now().minus(1, ChronoUnit.DAYS)), Date()) shouldBe events.take(24).asReversed()
        }

        "should fetch an event by event type" {
            val event = Event(UUID.randomUUID(), Date.from(Instant.now()), "selected_by", UUID.randomUUID(), "body of event".toByteBuffer())
            eventRepo.create(event) shouldBe true
            eventRepo.getForUser(event.resourceId, event.userId) shouldBe listOf(event)
        }

        "should not two events at the same time" {
            val event1 = Event(UUID.randomUUID(), Date.from(Instant.now()), "event_type1", UUID.randomUUID(), "event 1".toByteBuffer())
            val event2 = event1.copy(eventType = "event_type2", userId = UUID.randomUUID(), body ="event 2".toByteBuffer())
            eventRepo.create(event1)
            eventRepo.create(event2)
            eventRepo.get(event1.resourceId) shouldBe  listOf(event1)
        }
    }
}