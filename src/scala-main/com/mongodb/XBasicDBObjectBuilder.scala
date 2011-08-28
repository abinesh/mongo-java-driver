package com.mongodb

import java.util.LinkedList
object XBasicDBObjectBuilder {
  def start() = new XBasicDBObjectBuilder

  def start(k: String, v: AnyRef) = (new XBasicDBObjectBuilder).add(k, v)

  def start(m: java.util.Map[_, _]) = {
    val b = new XBasicDBObjectBuilder
    import scala.collection.JavaConverters._
    m.asScala.foreach {
      case (key, value) => b.add(key.toString, value.asInstanceOf[AnyRef])
    }
    b
  }
}

class XBasicDBObjectBuilder {
  private final val _stack = new LinkedList[DBObject]
  _stack.add(new BasicDBObject)

  def append(key: String, v: AnyRef) = {
    _cur.put(key, v)
    this
  }

  def add(key: String, v: AnyRef) = append(key, v)

  def push(key: String) = {
    val o = new BasicDBObject
    _cur.put(key, o)
    _stack.addLast(o)
    this
  }

  def pop = {
    if (_stack.size <= 1) throw new IllegalArgumentException("can't pop last element")
    _stack.removeLast()
    this
  }

  def get = _stack.getFirst

  def isEmpty = (_stack.getFirst.asInstanceOf[BasicDBObject]).size == 0

  private def _cur = _stack.getLast
}

