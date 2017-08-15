package dev.yn.event.mother.aggregate

import dev.yn.event.domain.AggregateEvent
import java.util.*
import dev.yn.util.date.*
import dev.yn.util.string.toByteBuffer

val resourceId1 = UUID.randomUUID()
val resourceId2 = UUID.randomUUID()
val date1 = "2017-05-01"
val date2 = "2017-05-02"
val date3 = "2017-05-03"
val date4 = "2017-05-04"
val date5 = "2017-05-05"
val date6 = "2017-05-06"
val date7 = "2017-05-07"
val date8 = "2017-05-08"
val date9 = "2017-05-09"

val eventTypes1 = listOf("e1a", "e1b")
val eventTypes2 = listOf("e2a")
val eventTypes3 = listOf("e3a", "e3b", "e3c")
val eventTypes4 = listOf("e4a", "e4b", "e4c", "e5d")
val eventTypes5 = listOf("e5a", "e5b", "e5c")
val eventTypes6 = listOf("e6a")
val eventTypes7 = listOf("e7a", "e7b")
val eventTypes8 = listOf("e8a", "e8b")

val user1 = UUID.randomUUID()
val user2 = UUID.randomUUID()
val user3 = UUID.randomUUID()

val aggregateEvents1 = listOf(
        AggregateEvent(resourceId1, dayFormat.parse(date1), eventTypes1, listOf(user1), "event 1".toByteBuffer()),
        AggregateEvent(resourceId1, dayFormat.parse(date2), eventTypes2, listOf(user2), "event 2".toByteBuffer()),
        AggregateEvent(resourceId1, dayFormat.parse(date3), eventTypes3, listOf(user3, user1), "event 3".toByteBuffer()),
        AggregateEvent(resourceId1, dayFormat.parse(date4), eventTypes4, listOf(user1, user2, user3), "event 4".toByteBuffer()),
        AggregateEvent(resourceId1, dayFormat.parse(date5), eventTypes5, listOf(user1, user2), "event 5".toByteBuffer()),
        AggregateEvent(resourceId1, dayFormat.parse(date6), eventTypes6, listOf(user3), "event 6".toByteBuffer()),
        AggregateEvent(resourceId1, dayFormat.parse(date7), eventTypes7, listOf(user1, user2), "event 7".toByteBuffer()),
        AggregateEvent(resourceId1, dayFormat.parse(date8), eventTypes8, listOf(user3), "event 8".toByteBuffer())
)

val aggregateEvents2 = listOf(
        AggregateEvent(resourceId2, dayFormat.parse(date1), eventTypes1, listOf(user1), "event 1".toByteBuffer()),
        AggregateEvent(resourceId2, dayFormat.parse(date3), eventTypes3, listOf(user2), "event 2".toByteBuffer()),
        AggregateEvent(resourceId2, dayFormat.parse(date4), eventTypes2, listOf(user3, user1), "event 3".toByteBuffer()),
        AggregateEvent(resourceId2, dayFormat.parse(date5), eventTypes7, listOf(user1, user2, user3), "event 4".toByteBuffer()),
        AggregateEvent(resourceId2, dayFormat.parse(date6), eventTypes6, listOf(user1, user2), "event 5".toByteBuffer()),
        AggregateEvent(resourceId2, dayFormat.parse(date7), eventTypes5, listOf(user3), "event 6".toByteBuffer()),
        AggregateEvent(resourceId2, dayFormat.parse(date8), eventTypes8, listOf(user1, user2), "event 7".toByteBuffer()),
        AggregateEvent(resourceId2, dayFormat.parse(date9), eventTypes4, listOf(user3), "event 8".toByteBuffer())
)