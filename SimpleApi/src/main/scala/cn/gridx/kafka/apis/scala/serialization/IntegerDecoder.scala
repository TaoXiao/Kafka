package cn.gridx.kafka.apis.scala.serialization

import kafka.serializer.Decoder

/**
 * Created by tao on 7/1/15.
 */
class IntegerDecoder extends Decoder[Int] {
    def fromBytes(bytes: Array[Byte]): Int = {
        if (None == bytes || bytes.isEmpty || bytes.size != 4)
            throw new IllegalArgumentException("The bytes to be decoded is invalid")
        else
            return java.nio.ByteBuffer.wrap(bytes).getInt
    }
}
