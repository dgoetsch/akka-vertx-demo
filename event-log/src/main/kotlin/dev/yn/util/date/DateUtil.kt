/*
 *    Copyright 2017 Devyn Goetsch
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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