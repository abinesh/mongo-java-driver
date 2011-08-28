package com.mongodb

import util.TestCase
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{FunSuite, BeforeAndAfterAll}

class XErrorTest extends TestCase with FunSuite with BeforeAndAfterAll with ShouldMatchers {
  private[mongodb] var _db: DB = null

  override def beforeAll() {
    cleanupMongo = new Mongo("127.0.0.1")
    cleanupDB = "com_mongodb_unittest_ErrorTest"
    _db = cleanupMongo.getDB(cleanupDB)
  }

  test("test last error") {
    _db.resetError
    _db.getLastError.get("err") should equal(null)

    _db.forceError
    _db.getLastError.get("err") should not equal (null)

    _db.resetError
    _db.getLastError.get("err") should equal(null)
  }

  test("last error with concern") {
    _db.resetError
    val cr = _db.getLastError(WriteConcern.FSYNC_SAFE)
    cr.get("err") should equal(null)
    (cr.containsField("fsyncFiles") || cr.containsField("waited")) should be(true)
  }

  test("last error with concern and w"){
      if (false) {
        _db.resetError
        val cr = _db.getLastError(WriteConcern.REPLICAS_SAFE)
         cr.get("err") should equal(null)
        cr.containsField("wtime") should be(true)
      }
    }

  test("previous error"){
    _db.resetError
     _db.getLastError.get("err") should equal(null)
     _db.getPreviousError.get("err") should equal(null)

    _db.forceError
    _db.getLastError.get("err") should not equal(null)
    _db.getPreviousError.get("err") should not equal(null)

    _db.getCollection("misc").insert(new BasicDBObject("foo", 1))
    _db.getLastError.get("err") should equal(null)
    _db.getPreviousError.get("err") should not equal(null)

    _db.resetError
    _db.getLastError.get("err") should equal(null)
    _db.getPreviousError.get("err") should equal(null)
  }
  }