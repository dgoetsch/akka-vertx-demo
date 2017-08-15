package dev.yn.cassandra

import com.datastax.driver.core.*
import java.util.*

/**
 *
 * Lovingly copied from,
 * http://www.devjavasource.com/cassandra/cassandra-paging-java-example/
 *
 * lightly modified
 */
object CassandraPaging {
    fun fetchRowsWithPage(statement: RegularStatement, offset: Int, limit: Int, session: Session): List<Row> {
        val result = skipRows(statement, offset, limit, session)
        return getRows(result, offset, limit)
    }

    private fun skipRows(statement: Statement, offset: Int, limit: Int, session: Session): ResultSet? {
        var statement = statement
        var result: ResultSet? = null
        val skippingPages = getPageNumber(offset, limit)
        var savingPageState: String? = null
        statement.setFetchSize(limit)
        var isEnd = false
        for (i in 0..skippingPages - 1) {
            if (null != savingPageState) {
                statement = statement.setPagingState(PagingState.fromString(savingPageState))
            }
            result = session.execute(statement)
            val pagingState = result.executionInfo.pagingState
            if (null != pagingState) {
                savingPageState = result.executionInfo.pagingState.toString()
            }

            if (result.isFullyFetched && null == pagingState) {
                // if hit the end more than once, then nothing to return,
                // otherwise, mark the isEnd to 'true'
                if (true == isEnd) {
                    return null
                } else {
                    isEnd = true
                }
            }
        }
        return result
    }
    private fun getPageNumber(offset: Int, limit: Int): Int {
        if (offset < 0) {
            return 1
        }
        var page = 1
        if (offset > limit - 1) {
            page = (offset / limit) + 1
        }
        return page
    }

    private fun getRows(result: ResultSet?, offset: Int, limit: Int): List<Row> {
        val rows = ArrayList<Row>(limit)
        if (null == result) {
            return rows
        }
        val skippingRows = (offset) % limit
        var index = 0
        val iter = result!!.iterator()
        while (iter.hasNext() && rows.size < limit) {
            val row = iter.next()
            if (index >= skippingRows) {
                rows.add(row)
            }
            index++
        }
        return rows
    }
}