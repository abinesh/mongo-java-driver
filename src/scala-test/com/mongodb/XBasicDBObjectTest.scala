package com.mongodb

import util.JSON
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FunSuite

class XBasicDBObjectTest extends FunSuite with ShouldMatchers {

  private def anyRef(n: Int) = n.asInstanceOf[AnyRef]

  test("test basic") {
    val a = new XBasicDBObject("x", anyRef(1))
    a should be(new XBasicDBObject("x", anyRef(1)))
    a should be(JSON.parse("{ 'x' : 1 }"))
  }

  test("test basic 2") {
    val a = new XBasicDBObject("x", anyRef(1))
    a should be(XBasicDBObjectBuilder.start().append("x", anyRef(1)).get)
    a should be(JSON.parse("{ 'x' : 1 }"))
    a should not be (JSON.parse("{ 'x' : 2 }"))
  }

  //  public void testBuilderIsEmpty() {
  test("test builder is empty") {
    val b = XBasicDBObjectBuilder.start();
    b.isEmpty should be(true)
    b.append("a", anyRef(1))
    b.isEmpty should be(false)
    b.get should be(JSON.parse("{ 'a' : 1 }"))
  }

  test("test builder is nested") {
    val b = XBasicDBObjectBuilder.start();
    b.add("a", anyRef(1));
    b.push("b").append("c", anyRef(2)).pop;
    val a = b.get;
    a should be(JSON.parse("{ 'a' : 1, 'b' : { 'c' : 2 } }"));
  }

  test("test down 1") {

    val b = XBasicDBObjectBuilder.start();
    b.append("x", anyRef(1));
    b.push("y");
    b.append("a", anyRef(2));
    b.pop;
    b.push("z");
    b.append("b", anyRef(3));

    val x = b.get;
    val y = JSON.parse("{ 'x' : 1 , 'y' : { 'a' : 2 } , 'z' : { 'b' : 3 } }");

    x should be(y)
  }

  private def _equal(x: XBasicDBObject, y: XBasicDBObject) {
    x should be(y)
    y should be(x)
  }

  private def _notequal(x: XBasicDBObject, y: XBasicDBObject) {
    x should not be (y)
    y should not be (x)
  }

  test("test equality") {
    val a = new XBasicDBObject
    val b = new XBasicDBObject
    _equal(a, b)
    a.put("x", anyRef(1))
    _notequal(a, b)
    b.put("x", anyRef(1))
    _equal(a, b)
    a.removeField("x")
    _notequal(a, b)
    b.removeField("x")
    _equal(a, b)
    a.put("x", null)
    b.put("x", anyRef(2))
    _notequal(a, b)
    a.put("x", anyRef(2))
    b.put("x", null)
    _notequal(a, b)
  }
}