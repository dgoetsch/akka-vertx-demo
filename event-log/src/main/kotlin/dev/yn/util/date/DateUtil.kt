package dev.yn.util.date

import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.util.*

val dayFormat = SimpleDateFormat("yyyy-MM-dd")

tailrec fun constructDayList(earliest: Date, latest: Date, currentResult: MutableList<String> = mutableListOf()): List<String> {
    val next = dayFormat.format(earliest)
    val last = dayFormat.format(latest)

    if(next > last) return currentResult
    if(next == last) return currentResult + next
    else {
        currentResult.add(next)
        return constructDayList(Date.from(earliest.toInstant().plus(1, ChronoUnit.DAYS)), latest, currentResult)
    }
}

fun earliest(d1: Date, d2: Date): Date {
    return if(d1.before(d2)) d1 else d2
}

fun latest(d1: Date, d2: Date): Date {
    return if(d1.after(d2)) d1 else d2
}