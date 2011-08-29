import com.mongodb._
import com.mongodb.util.TestCase
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.util._
import java.util.regex.Pattern
import org.bson.types.ObjectId
import org.bson.{BasicBSONDecoder, BSONDecoder, BSONObject, BSON}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.collection.JavaConverters._


class ByteTest extends TestCase with FunSuite with BeforeAndAfterAll with ShouldMatchers {

  private var _db: DB = null

  override def beforeAll() {
    cleanupMongo = new Mongo("127.0.0.1")
    cleanupDB = "com_mongodb_unittest_ByteTest"
    _db = cleanupMongo.getDB(cleanupDB)
  }

  test("test object 1") {
    val o = new BasicDBObject
    o.put("eliot", "horowitz")
    o.put("num", 517)
    val read = BSON.decode(BSON.encode(o))
    read.get("eliot").toString should equal("horowitz")
    (read.get("num").asInstanceOf[Integer]).doubleValue should equal(517.0)
  }

  test("test string") {
    val eliot = java.net.URLDecoder.decode("horowitza%C3%BCa", "UTF-8")
    val o = new BasicDBObject
    o.put("eliot", eliot)
    o.put("num", 517)
    val read = BSON.decode(BSON.encode(o))
    read.get("eliot").toString should equal(eliot)
    (read.get("num").asInstanceOf[Integer]).doubleValue should equal(517.0)
  }

  test("test object 2") {
    val o = new BasicDBObject
    o.put("eliot", "horowitz")
    o.put("num", 517.3)
    o.put("z", "y")
    o.put("asd", null)
    val o2 = new BasicDBObject
    o2.put("a", "b")
    o2.put("b", "a")
    o.put("next", o2)
    val read = BSON.decode(BSON.encode(o))
    read.get("eliot").toString should equal("horowitz")
    (read.get("num").asInstanceOf[Double]).doubleValue should equal(517.3)
    (read.get("next").asInstanceOf[BSONObject]).get("a").toString should equal("b")
    (read.get("next").asInstanceOf[BSONObject]).get("b").toString should equal("a")
    read.get("z").toString should equal("y")
    read.keySet.size should equal(o.keySet.size)
  }

  test("test array 1") {
    val o = new BasicDBObject
    o.put("eliot", "horowitz")
    o.put("num", 517)
    o.put("z", "y")
    o.put("asd", null)
    o.put("myt", true)
    o.put("myf", false)
    val a = new ArrayList[String]
    a.add("A")
    a.add("B")
    a.add("C")
    o.put("a", a)
    o.put("d", new Date)
    val read = BSON.decode(BSON.encode(o))
    read.get("eliot").toString should equal("horowitz")
    (read.get("num").asInstanceOf[Integer]).intValue should equal(517)
    read.get("z").toString should equal("y")
    read.keySet.size should equal(o.keySet.size)
    a.size should equal(3)
    (read.get("a").asInstanceOf[java.util.List[_]]).size should equal(a.size)
    (read.get("a").asInstanceOf[java.util.List[String]]).get(0).toString should equal("A")
    (read.get("a").asInstanceOf[java.util.List[String]]).get(1).toString should equal("B")
    (read.get("a").asInstanceOf[java.util.List[String]]).get(2).toString should equal("C")
    (read.get("d").asInstanceOf[Date]).getTime should equal((o.get("d").asInstanceOf[Date]).getTime)
    o.get("myt").asInstanceOf[Boolean] should equal(true)
    o.get("myf").asInstanceOf[Boolean] should equal(false)
  }

  test("test array 2") {
    val x = new BasicDBObject
    x.put("a", Array[String]("a", "b", "c"))
    x.put("b", Array[Int](1, 2, 3))
    val y = BSON.decode(BSON.encode(x))
    val listA = y.get("a").asInstanceOf[java.util.List[String]]
    listA.size should equal(3)
    listA.get(0) should equal("a")
    listA.get(1) should equal("b")
    listA.get(2) should equal("c")
    val listB = y.get("b").asInstanceOf[java.util.List[Int]]
    listB.size should equal(3)
    listB.get(0) should equal(1)
    listB.get(1) should equal(2)
    listB.get(2) should equal(3)
  }

  //  @Test(groups = Array("basic")) def testObjcetId: Unit = {
  test("test object id") {
    (new ObjectId().compareTo(new ObjectId) < 0) should be(true)
    ((new ObjectId(0, 0, 0)).compareTo(new ObjectId) < 0) should be(true)
    ((new ObjectId(0, 0, 0)).compareTo(new ObjectId(0, 0, 1)) < 0) should be(true)
    ((new ObjectId(5, 5, 5)).compareTo(new ObjectId(5, 5, 6)) < 0) should be(true)
    ((new ObjectId(5, 5, 5)).compareTo(new ObjectId(5, 6, 5)) < 0) should be(true)
    ((new ObjectId(5, 5, 5)).compareTo(new ObjectId(6, 5, 5)) < 0) should be(true)
  }

  //  @Test(groups = Array("basic")) def testBinary: Unit = {
  test("test binary") {
    val barray = for (i <- 0 until 256) yield (i - 128).asInstanceOf[Byte];

    val o = new BasicDBObject
    o.put("bytes", barray.toArray)

    val encoded = BSON.encode(o)
    encoded.length should be(273)

    val read = BSON.decode(encoded)
    val b = read.get("bytes").asInstanceOf[Array[Byte]]

    b.length should be(barray.length)
    for (i <- 0 until 256) b(i) should be(barray(i))
    read.keySet.size should be(o.keySet.size)
  }

  private def go(o: DBObject, serialized_len: Int) {
    go(o, serialized_len, 0)
  }

  private def go(o: DBObject, serialized_len: Int, transient_fields: Int) {
    val encoded = BSON.encode(o)
    encoded.length should be(serialized_len)
    val read = BSON.decode(encoded)
    read.keySet.size should be(o.keySet.size - transient_fields)
    if (transient_fields == 0) read should be(o)
  }

  //  @Test(groups = Array("basic")) def testEncodeDecode: Unit = {
  test("test encode decode") {
    val t = new java.util.ArrayList[String]
    var obj: AnyRef = null
    var threw: Boolean = false
    try {
      go(null.asInstanceOf[DBObject], 0)
    }
    catch {
      case e: RuntimeException => {
        threw = true
      }
    }
    threw should be(true)
    threw = false
    var o: DBObject = new BasicDBObject
    var serialized_len: Int = 5
    go(o, 5)
    o.put("_id", obj)
    Bytes.getType(obj) should equal(BSON.NULL)
    go(o, 10)
    obj = new java.util.ArrayList[String]
    o.put("_id", obj)
    Bytes.getType(obj) should equal(BSON.ARRAY)
    go(o, 15)
    obj = new ObjectId
    o.put("_id", obj)
    Bytes.getType(obj) should equal(BSON.OID)
    go(o, 22)
    try {
      obj = _db.getCollection("test")
      o.put("_id", obj)
      Bytes.getType(obj) should equal(0)

      go(o, 22)
    }
    catch {
      case e: RuntimeException => {
        threw = true
      }
    }
    threw should be(true)
    threw = false
    t.add("collection")
    o = new BasicDBObject
    o.put("collection", _db.getCollection("test"))
    o.put("_transientFields", t)
    go(o, 5, 2)
    t.clear
    o = new BasicDBObject
    o.put("_transientFields", new ArrayList[String])
    go(o, 5, 1)
    t.add("foo")
    o = new BasicDBObject
    o.put("_transientFields", t)
    o.put("foo", "bar")
    go(o, 5, 2)
    t.clear
    o = new BasicDBObject
    o.put("z", "")
    go(o, 13)
    t.clear
    obj = 5.asInstanceOf[AnyRef]
    o = new BasicDBObject
    o.put("$where", obj)
    Bytes.getType(obj) should be(BSON.NUMBER_INT)
    go(o, 17)
  }

  //  @Test(groups = Array("basic")) def testPatternFlags: Unit = {
  test("test pattern flags") {
    var threw: Boolean = false
    BSON.regexFlags("") should be(0)
    BSON.regexFlags(0) should be("")
    try {
      BSON.regexFlags("f")
    }
    catch {
      case e: RuntimeException => {
        threw = true
      }
    }
    threw should be(true)
    threw = false
    try {
      BSON.regexFlags(513)
    }
    catch {
      case e: RuntimeException => {
        threw = true
      }
    }
    threw should be(true)
    var lotsoflags: Pattern = Pattern.compile("foo", Pattern.CANON_EQ | Pattern.DOTALL | Pattern.CASE_INSENSITIVE | Pattern.UNIX_LINES | Pattern.MULTILINE | Pattern.LITERAL | Pattern.UNICODE_CASE | Pattern.COMMENTS | 256)
    var s: String = BSON.regexFlags(lotsoflags.flags)
    var prev: Char = s.charAt(0)
    for (i <- 1 until s.length()) {
      val current = s.charAt(i);
      (prev < current) should be(true);
      prev = current;
    }
    val check = BSON.regexFlags(s)
    lotsoflags.flags should be(check)
  }

  //  @Test(groups = Array("basic")) def testPattern: Unit = {
  test("test pattern") {
    var p: Pattern = Pattern.compile("([a-zA-Z0-9_-])([a-zA-Z0-9_.-]*)@(((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\\.))")
    var o: DBObject = new BasicDBObject
    o.put("p", p)
    var read: BSONObject = BSON.decode(BSON.encode(o))
    var p2: Pattern = read.get("p").asInstanceOf[Pattern]
    p2.pattern should be(p.pattern)
    read.keySet.size should be(o.keySet.size)
  }

  //  @Test(groups = Array("basic")) def testLong: Unit = {
  test("test long") {
    var s: Long = -9223372036854775808l
    var m: Long = 1l
    var l: Long = 9223372036854775807l
    var obj: DBObject = BasicDBObjectBuilder.start.add("s", s).add("m", m).add("l", l).get
    var c: DBCollection = _db.getCollection("test")
    c.drop
    c.insert(obj)
    var r: DBObject = c.findOne
    r.get("s") should equal(-9223372036854775808l)
    r.get("m") should equal(1l)
    r.get("l") should equal(9223372036854775807l)
  }

  //  @Test(groups = Array("basic")) def testIdOrder: Unit = {
  test("test id order") {
    var c: DBCollection = _db.getCollection("testidorder")
    c.drop
    var x: BasicDBObject = new BasicDBObject
    x.put("a", 5)
    x.put("_id", 6)
    val y = new BasicDBObject
    y.put("b", 7)
    y.put("_id", 8)
    x.put("c", y)
    c.insert(x)
    val out = c.findOne
    _testKeys(Array[String]("_id", "a", "c"), out.keySet.asScala)
    _testKeys(Array[String]("b", "_id"), (out.get("c").asInstanceOf[DBObject]).keySet.asScala)
  }

  private def _testKeys(want: Array[String], got: scala.collection.mutable.Set[String]) {
    want.length should equal(got.size)
    var pos: Int = 0
    got.foreach {
      s =>
        want(pos) should equal(s)
        pos += 1;
    }
  }

  //  @Test(groups = Array("basic")) def testBytes2: Unit = {
  test("test bytes 2") {
    var x: DBObject = BasicDBObjectBuilder.start("x", 1).add("y", "asdasd").get
    var b: Array[Byte] = BSON.encode(x)
    x should be(BSON.decode(b))
  }

  //  @Test def testMany: Unit = {
  test("test many") {
    val orig = new BasicDBObject
    orig.put("a", 5)
    orig.put("ab", 5.1)
    orig.put("abc", 5123L)
    orig.put("abcd", "asdasdasd")
    orig.put("abcde", "asdasdasdasdasdasdasdasd")
    val list = new java.util.ArrayList[String]
    list.add("asdasdasdasdasdasdasdasd")
    list.add("asdasdasdasdasdasdasdasd")
    orig.put("abcdef", list)

    var b: Array[Byte] = BSON.encode(orig)
    val n: Int = 1000
    var out: ByteArrayOutputStream = new ByteArrayOutputStream
    for (i <- 1 until n) out.write(b)

    var in: ByteArrayInputStream = new ByteArrayInputStream(out.toByteArray)
    var d: BSONDecoder = new BasicBSONDecoder
    for (i <- 1 until n) {
      var x: BSONObject = d.readObject(in)
      x.equals(orig) should be(true)
    }
    in.read should be(-1)

  }

  private def _fix(x: Int): Int = {
    if (x < 0) -1 else if (x > 0) 1 else 0
  }

  //  @Test def testObjcetIdCompare: Unit = {
  test("test object id compare") {
    var r: Random = new Random(171717)
    var l: List[ObjectId] = new ArrayList[ObjectId]
    for (i <- 1 until 10000)
      l.add(new ObjectId(new Date(Math.abs(r.nextLong)), Math.abs(r.nextInt), Math.abs(r.nextInt)))
    for (i <- 1 until l.size) {
      var a: Int = _fix(l.get(0).compareTo(l.get(i)))
      var b: Int = _fix(l.get(0).toString.compareTo(l.get(i).toString))
      if (a != b)
        throw new RuntimeException("broken [" + l.get(0) + "] [" + l.get(i) + "] a: " + a + " b: " + b)
    }

    var c: DBCollection = _db.getCollection("testObjcetIdCompare")
    c.drop
    for (o <- l.asScala) {
      c.insert(new BasicDBObject("_id", o))
    }
    Collections.sort(l)
    var out: List[DBObject] = c.find.sort(new BasicDBObject("_id", 1)).toArray
    l.size should equal(out.size)
    for (i <- 1 until l.size) {
      l.get(i) should equal(out.get(i).get("_id"))
    }
  }
}

