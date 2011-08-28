package org.bson

import io.PoolOutputBuffer
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
class XPoolOutputBufferTest extends FlatSpec with ShouldMatchers {

  it should "test basic stuff" in {
    val buf: PoolOutputBuffer = new PoolOutputBuffer
    buf.write("eliot".getBytes)
    buf.getPosition should equal(5)
    buf.size should equal(5)

    buf.asString() should equal("eliot")

    buf.setPosition(2)
    buf.write("z".getBytes)
    buf.asString() should equal("elzot")

    buf.seekEnd
    buf.write("foo".getBytes)
    buf.asString() should equal("elzotfoo")

    buf.seekStart
    buf.write("bar".getBytes)
    buf.asString() should equal("barotfoo")
  }


}