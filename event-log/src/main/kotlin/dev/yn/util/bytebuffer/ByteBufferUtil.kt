package dev.yn.util.bytebuffer

import java.nio.ByteBuffer

fun ByteBuffer.read(): ByteArray {
    return ByteArray(this.remaining()).let { arr ->
        this.get(arr)
        arr
    }
}