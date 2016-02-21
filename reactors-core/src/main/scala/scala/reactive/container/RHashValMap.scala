package scala.reactive
package container



import scala.reflect.ClassTag



class RHashValMap[@spec(Int, Long, Double) K, @spec(Int, Long, Double) V](
  implicit val emptyKey: Arrayable[K],
  implicit val emptyVal: Arrayable[V]
) extends RContainer[(K, V)]
with RBuilder[(K, V), RHashValMap[K, V]]
with ValPairBuilder[K, V, RHashValMap[K, V]] {
  private var keytable: Array[K] = null
  private var valtable: Array[V] = null
  private var sz = 0
  private[reactive] var insertsEmitter: Events.Emitter[(K, V)] = null
  private[reactive] var removesEmitter: Events.Emitter[(K, V)] = null

  protected def init(ek: Arrayable[K], ev: Arrayable[V]) {
    keytable = emptyKey.newArray(RHashValMap.initSize)
    valtable = emptyVal.newArray(RHashValMap.initSize)
    insertsEmitter = new Events.Emitter[(K, V)]
    removesEmitter = new Events.Emitter[(K, V)]
  }

  init(emptyKey, emptyVal)

  def nil: V = emptyVal.nil

  def inserts: Events[(K, V)] = insertsEmitter
  def removes: Events[(K, V)] = removesEmitter

  def builder: RBuilder[(K, V), RHashValMap[K, V]] = this

  def +=(kv: (K, V)) = {
    insertPair(kv._1, kv._2)
  }

  def -=(kv: (K, V)) = {
    removePair(kv._1, kv._2)
  }

  def insertPair(k: K, v: V) = {
    insert(k, v)
    true
  }

  def removePair(k: K, v: V) = {
    delete(k, v) != emptyVal.nil
  }

  def container = this

  val react = new RHashValMap.Lifted[K, V](this)

  def foreachPair(f: (K, V) => Unit) {
    var i = 0
    while (i < keytable.length) {
      val k = keytable(i)
      if (k != emptyKey.nil) {
        val v = valtable(i)
        f(k, v)
      }
      i += 1
    }
  }

  def foreach(f: ((K, V)) => Unit) {
    foreachPair { (k, v) =>
      f((k, v))
    }
  }

  private def lookup(k: K): V = {
    var pos = index(k)
    val nil = emptyKey.nil
    var curr = keytable(pos)

    while (curr != nil && curr != k) {
      pos = (pos + 1) % keytable.length
      curr = keytable(pos)
    }

    if (curr == nil) emptyVal.nil
    else valtable(pos)
  }

  private def insert(k: K, v: V, notify: Boolean = true): V = {
    checkResize()

    var pos = index(k)
    val nil = emptyKey.nil
    var curr = keytable(pos)
    assert(k != nil)

    while (curr != nil && curr != k) {
      pos = (pos + 1) % keytable.length
      curr = keytable(pos)
    }

    val previousValue = valtable(pos)
    keytable(pos) = k
    valtable(pos) = v
    
    val keyAdded = curr == nil
    if (keyAdded) sz += 1
    else if (notify && removesEmitter.hasSubscriptions) removesEmitter.react((k, previousValue))
    if (notify && insertsEmitter.hasSubscriptions) insertsEmitter.react((k, v))

    previousValue
  }

  private def delete(k: K, expectedValue: V = emptyVal.nil): V = {
    var pos = index(k)
    val nil = emptyKey.nil
    var curr = keytable(pos)

    while (curr != nil && curr != k) {
      pos = (pos + 1) % keytable.length
      curr = keytable(pos)
    }

    if (curr == nil) emptyVal.nil
    else if (expectedValue == emptyVal.nil || expectedValue == curr) {
      val previousValue = valtable(pos)

      var h0 = pos
      var h1 = (h0 + 1) % keytable.length
      while (keytable(h1) != nil) {
        val h2 = index(keytable(h1))
        if (h2 != h1 && before(h2, h0)) {
          keytable(h0) = keytable(h1)
          valtable(h0) = valtable(h1)
          h0 = h1
        }
        h1 = (h1 + 1) % keytable.length
      }

      keytable(h0) = emptyKey.nil
      valtable(h0) = emptyVal.nil
      sz -= 1
      if (removesEmitter.hasSubscriptions) removesEmitter.react((k, previousValue))

      previousValue
    } else emptyVal.nil
  }

  private def checkResize() {
    if (sz * 1000 / RHashValMap.loadFactor > keytable.length) {
      val okeytable = keytable
      val ovaltable = valtable
      val ncapacity = keytable.length * 2
      keytable = emptyKey.newArray(ncapacity)
      valtable = emptyVal.newArray(ncapacity)
      sz = 0

      var pos = 0
      val nil = emptyKey.nil
      while (pos < okeytable.length) {
        val curr = okeytable(pos)
        if (curr != nil) {
          insert(curr, ovaltable(pos), false)
        }

        pos += 1
      }
    }
  }

  private def before(i: Int, j: Int) = {
    val d = keytable.length >> 1
    if (i <= j) j - i < d
    else i - j > d
  }

  private def index(k: K): Int = {
    val hc = k.##
    math.abs(scala.util.hashing.byteswap32(hc)) % keytable.length
  }

  def apply(key: K): V = lookup(key) match {
    case `emptyVal`.nil => throw new NoSuchElementException("key: " + key)
    case v => v
  }

  def applyOrElse(key: K, otherValue: V): V = lookup(key) match {
    case `emptyVal`.nil => otherValue
    case v => v
  }

  def applyOrNil(key: K): V = lookup(key)

  def get(key: K): Option[V] = lookup(key) match {
    case `emptyVal`.nil => None
    case v => Some(v)
  }

  def contains(key: K): Boolean = lookup(key) match {
    case `emptyVal`.nil => false
    case v => true
  }

  def update(key: K, value: V): Unit = insert(key, value)

  def remove(key: K): Boolean = delete(key) match {
    case `emptyVal`.nil => false
    case v => true
  }

  def clear() {
    var pos = 0
    val nil = emptyKey.nil
    while (pos < keytable.length) {
      if (keytable(pos) != nil) {
        val k = keytable(pos)
        val v = valtable(pos)

        keytable(pos) = emptyKey.nil
        valtable(pos) = emptyVal.nil
        sz -= 1
        if (removesEmitter.hasSubscriptions) removesEmitter.react((k, v))
      }

      pos += 1
    }
  }

  def size: Int = sz
  
}


object RHashValMap {

  def apply[@spec(Int, Long, Double) K: Arrayable, V: Arrayable] = new RHashValMap[K, V]

  class Lifted[@spec(Int, Long, Double) K, @spec(Int, Long, Double) V](val container: RHashValMap[K, V]) extends RContainer.Lifted[(K, V)]

  val initSize = 16

  val loadFactor = 400

  implicit def factory[@spec(Int, Long, Double) K: Arrayable, @spec(Int, Long, Double) V: Arrayable] = new RBuilder.Factory[(K, V), RHashValMap[K, V]] {
    def apply() = RHashValMap[K, V]
  }

  implicit def valPairFactory[@spec(Int, Long, Double) K: Arrayable, @spec(Int, Long, Double) V: Arrayable] = new ValPairBuilder.Factory[K, V, RHashValMap[K, V]] {
    def apply() = RHashValMap[K, V]
  }
}




