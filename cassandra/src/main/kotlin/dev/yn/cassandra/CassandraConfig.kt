package dev.yn.cassandra

import com.natpryce.konfig.*

object CassandraConfig {
    object cassandra: PropertyGroup() {
        val nodes by listType(stringType)
    }
}