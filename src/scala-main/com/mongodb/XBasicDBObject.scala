package com.mongodb

import util.JSON
import org.bson.BasicBSONObject

object XBasicDBObject {
  private val serialVersionUID = -4415279469780082174L

  private def create() = new BasicBSONObject

  private def create(size: Int) = new BasicBSONObject(size)

  private def create(key: String, value: AnyRef) = new BasicBSONObject(key, value)
}

class XBasicDBObject(m: java.util.Map[_, _]) extends BasicBSONObject(m) with DBObject {

  private var _isPartialObject = false

  def this() = this (XBasicDBObject.create())

  def this(size: Int) = this (XBasicDBObject.create(size))

  def this(key: String, value: AnyRef) = this (XBasicDBObject.create(key, value))

  def isPartialObject = _isPartialObject

  def markAsPartialObject() {
    _isPartialObject = true
  }

  override def toString = JSON.serialize(this)

  override def append(key: String, v: AnyRef) = {
    put(key, v)
    this
  }

  def copy: XBasicDBObject = {
    val newObject = new XBasicDBObject(this.toMap)
    import scala.collection.JavaConverters._
    for (field <- keySet().asScala) {
      val v = get(field)
      if (v.isInstanceOf[XBasicDBObject]) {
        newObject.put(field, (v.asInstanceOf[XBasicDBObject]).copy)
      }
      else if (v.isInstanceOf[BasicDBList]) {
        newObject.put(field, (v.asInstanceOf[BasicDBList]).copy)
      }
    }
    newObject
  }
}

