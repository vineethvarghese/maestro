package au.com.cba.omnia.maestro.core
package hive

import test.Arbitraries._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

import org.scalacheck._, Arbitrary._

import partition.Partition
import data.Field
import codec.Describe

// example's imports
import com.twitter.scrooge.{
  TFieldBlob, ThriftException, ThriftStruct, ThriftStructCodec3, ThriftStructFieldInfo, ThriftUtil}
import org.apache.thrift.protocol._
import org.apache.thrift.transport.{TMemoryBuffer, TTransport}
import java.nio.ByteBuffer
import java.util.Arrays
import scala.collection.immutable.{Map => immutable$Map}
import scala.collection.mutable.Builder
import scala.collection.mutable.{
  ArrayBuffer => mutable$ArrayBuffer, Buffer => mutable$Buffer,
  HashMap => mutable$HashMap, HashSet => mutable$HashSet}
import scala.collection.{Map, Set}


object TableDescriptorSpec extends test.Spec { def is = s2"""

TableDescriptor Properties
==========================

  mapType is total and correct                              $tmap
  createHiveDescriptor is total and correct                 $tdesc
  tablePath returns only valid paths                        $tpath
  name uses the type's describe instance                    $tname
  qualifiedName returns a valid name                        $tqname
  createSchema returns a valid scheme                       $tscheme

"""

  implicit def customerDescribe = Describe[Customer]("Customer", List(Tuple2("CustomerId", Customer.CustomerIdField), Tuple2("CustomerName", Customer.CustomerNameField), Tuple2("CustomerAcct", Customer.CustomerAcctField), Tuple2("CustomerCat", Customer.CustomerCatField), Tuple2("CustomerSubCat", Customer.CustomerSubCatField), Tuple2("CustomerBalance", Customer.CustomerBalanceField), Tuple2("EffectiveDate", Customer.EffectiveDateField)))

  val byDate        = TableDescriptor("test", Partition.byDate(Fields1.EffectiveDate))
  val byCategory    = TableDescriptor("test", Partition.byDate(Fields1.CustomerSubCat))

  def tmap = prop((tt: List[Byte]) => {
    tt.flatMap(byDate.mapType(_))
    ok
  })(implicitly, Arbitrary(Gen.listOf(TTypeGen)), implicitly)

  def tdesc = prop((xs: Boolean) => xs == false).pendingUntilFixed

  def tpath = prop((xs: Boolean) => xs == false).pendingUntilFixed

  def tname = prop((xs: Boolean) => xs == false).pendingUntilFixed

  def tqname = prop((xs: Boolean) => xs == false).pendingUntilFixed

  def tscheme = prop((xs: Boolean) => xs == false).pendingUntilFixed

}

// Generated fields1
object Fields1 extends scala.AnyRef {
 def CustomerId = Field[Customer, String]("CustomerId", ((x) => x._1));
 def CustomerName = Field[Customer, String]("CustomerName", ((x) => x._2));
 def CustomerAcct = Field[Customer, String]("CustomerAcct", ((x) => x._3));
 def CustomerCat = Field[Customer, String]("CustomerCat", ((x) => x._4));
 def CustomerSubCat = Field[Customer, String]("CustomerSubCat", ((x) => x._5));
 def CustomerBalance = Field[Customer, Int]("CustomerBalance", ((x) => x._6));
 def EffectiveDate = Field[Customer, String]("EffectiveDate", ((x) => x._7));
 def AllFields = List(CustomerId, CustomerName, CustomerAcct, CustomerCat, CustomerSubCat, CustomerBalance, EffectiveDate)
}

// Generated example
object Customer extends ThriftStructCodec3[Customer] {
  private val NoPassthroughFields = immutable$Map.empty[Short, TFieldBlob]
  val Struct = new TStruct("Customer")
  val CustomerIdField = new TField("CUSTOMER_ID", TType.STRING, 1)
  val CustomerIdFieldManifest = implicitly[Manifest[String]]
  val CustomerNameField = new TField("CUSTOMER_NAME", TType.STRING, 2)
  val CustomerNameFieldManifest = implicitly[Manifest[String]]
  val CustomerAcctField = new TField("CUSTOMER_ACCT", TType.STRING, 3)
  val CustomerAcctFieldManifest = implicitly[Manifest[String]]
  val CustomerCatField = new TField("CUSTOMER_CAT", TType.STRING, 4)
  val CustomerCatFieldManifest = implicitly[Manifest[String]]
  val CustomerSubCatField = new TField("CUSTOMER_SUB_CAT", TType.STRING, 5)
  val CustomerSubCatFieldManifest = implicitly[Manifest[String]]
  val CustomerBalanceField = new TField("CUSTOMER_BALANCE", TType.I32, 6)
  val CustomerBalanceFieldManifest = implicitly[Manifest[Int]]
  val EffectiveDateField = new TField("EFFECTIVE_DATE", TType.STRING, 7)
  val EffectiveDateFieldManifest = implicitly[Manifest[String]]

  /**
   * Field information in declaration order.
   */
  lazy val fieldInfos: scala.List[ThriftStructFieldInfo] = scala.List[ThriftStructFieldInfo](
    new ThriftStructFieldInfo(
      CustomerIdField,
      false,
      CustomerIdFieldManifest,
      None,
      None,
      immutable$Map(
      ),
      immutable$Map(
      )
    ),
    new ThriftStructFieldInfo(
      CustomerNameField,
      false,
      CustomerNameFieldManifest,
      None,
      None,
      immutable$Map(
      ),
      immutable$Map(
      )
    ),
    new ThriftStructFieldInfo(
      CustomerAcctField,
      false,
      CustomerAcctFieldManifest,
      None,
      None,
      immutable$Map(
      ),
      immutable$Map(
      )
    ),
    new ThriftStructFieldInfo(
      CustomerCatField,
      false,
      CustomerCatFieldManifest,
      None,
      None,
      immutable$Map(
      ),
      immutable$Map(
      )
    ),
    new ThriftStructFieldInfo(
      CustomerSubCatField,
      false,
      CustomerSubCatFieldManifest,
      None,
      None,
      immutable$Map(
      ),
      immutable$Map(
      )
    ),
    new ThriftStructFieldInfo(
      CustomerBalanceField,
      false,
      CustomerBalanceFieldManifest,
      None,
      None,
      immutable$Map(
      ),
      immutable$Map(
      )
    ),
    new ThriftStructFieldInfo(
      EffectiveDateField,
      false,
      EffectiveDateFieldManifest,
      None,
      None,
      immutable$Map(
      ),
      immutable$Map(
      )
    )
  )

  lazy val structAnnotations: immutable$Map[String, String] =
    immutable$Map[String, String](
    )

  /**
   * Checks that all required fields are non-null.
   */
  def validate(_item: Customer) {
  }

  override def encode(_item: Customer, _oproto: TProtocol) {
    _item.write(_oproto)
  }

  override def decode(_iprot: TProtocol): Customer = {
    var customerId: String = null
    var customerName: String = null
    var customerAcct: String = null
    var customerCat: String = null
    var customerSubCat: String = null
    var customerBalance: Int = 0
    var effectiveDate: String = null
    var _passthroughFields: Builder[(Short, TFieldBlob), immutable$Map[Short, TFieldBlob]] = null
    var _done = false

    _iprot.readStructBegin()
    while (!_done) {
      val _field = _iprot.readFieldBegin()
      if (_field.`type` == TType.STOP) {
        _done = true
      } else {
        _field.id match {
          case 1 =>
            _field.`type` match {
              case TType.STRING => {
                customerId = readCustomerIdValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.STRING
            
                throw new TProtocolException(
                  "Received wrong type for field 'customerId' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)
                  )
                )
            }
          case 2 =>
            _field.`type` match {
              case TType.STRING => {
                customerName = readCustomerNameValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.STRING
            
                throw new TProtocolException(
                  "Received wrong type for field 'customerName' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)
                  )
                )
            }
          case 3 =>
            _field.`type` match {
              case TType.STRING => {
                customerAcct = readCustomerAcctValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.STRING
            
                throw new TProtocolException(
                  "Received wrong type for field 'customerAcct' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)
                  )
                )
            }
          case 4 =>
            _field.`type` match {
              case TType.STRING => {
                customerCat = readCustomerCatValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.STRING
            
                throw new TProtocolException(
                  "Received wrong type for field 'customerCat' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)
                  )
                )
            }
          case 5 =>
            _field.`type` match {
              case TType.STRING => {
                customerSubCat = readCustomerSubCatValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.STRING
            
                throw new TProtocolException(
                  "Received wrong type for field 'customerSubCat' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)
                  )
                )
            }
          case 6 =>
            _field.`type` match {
              case TType.I32 => {
                customerBalance = readCustomerBalanceValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.I32
            
                throw new TProtocolException(
                  "Received wrong type for field 'customerBalance' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)
                  )
                )
            }
          case 7 =>
            _field.`type` match {
              case TType.STRING => {
                effectiveDate = readEffectiveDateValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.STRING
            
                throw new TProtocolException(
                  "Received wrong type for field 'effectiveDate' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)
                  )
                )
            }
          case _ =>
            if (_passthroughFields == null)
              _passthroughFields = immutable$Map.newBuilder[Short, TFieldBlob]
            _passthroughFields += (_field.id -> TFieldBlob.read(_field, _iprot))
        }
        _iprot.readFieldEnd()
      }
    }
    _iprot.readStructEnd()

    new Immutable(
      customerId,
      customerName,
      customerAcct,
      customerCat,
      customerSubCat,
      customerBalance,
      effectiveDate,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result()
    )
  }

  def apply(
    customerId: String,
    customerName: String,
    customerAcct: String,
    customerCat: String,
    customerSubCat: String,
    customerBalance: Int,
    effectiveDate: String
  ): Customer =
    new Immutable(
      customerId,
      customerName,
      customerAcct,
      customerCat,
      customerSubCat,
      customerBalance,
      effectiveDate
    )

  def unapply(_item: Customer): Option[Product7[String, String, String, String, String, Int, String]] = Some(_item)


  private def readCustomerIdValue(_iprot: TProtocol): String = {
    _iprot.readString()
  }

  private def writeCustomerIdField(customerId_item: String, _oprot: TProtocol) {
    _oprot.writeFieldBegin(CustomerIdField)
    writeCustomerIdValue(customerId_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeCustomerIdValue(customerId_item: String, _oprot: TProtocol) {
    _oprot.writeString(customerId_item)
  }

  private def readCustomerNameValue(_iprot: TProtocol): String = {
    _iprot.readString()
  }

  private def writeCustomerNameField(customerName_item: String, _oprot: TProtocol) {
    _oprot.writeFieldBegin(CustomerNameField)
    writeCustomerNameValue(customerName_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeCustomerNameValue(customerName_item: String, _oprot: TProtocol) {
    _oprot.writeString(customerName_item)
  }

  private def readCustomerAcctValue(_iprot: TProtocol): String = {
    _iprot.readString()
  }

  private def writeCustomerAcctField(customerAcct_item: String, _oprot: TProtocol) {
    _oprot.writeFieldBegin(CustomerAcctField)
    writeCustomerAcctValue(customerAcct_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeCustomerAcctValue(customerAcct_item: String, _oprot: TProtocol) {
    _oprot.writeString(customerAcct_item)
  }

  private def readCustomerCatValue(_iprot: TProtocol): String = {
    _iprot.readString()
  }

  private def writeCustomerCatField(customerCat_item: String, _oprot: TProtocol) {
    _oprot.writeFieldBegin(CustomerCatField)
    writeCustomerCatValue(customerCat_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeCustomerCatValue(customerCat_item: String, _oprot: TProtocol) {
    _oprot.writeString(customerCat_item)
  }

  private def readCustomerSubCatValue(_iprot: TProtocol): String = {
    _iprot.readString()
  }

  private def writeCustomerSubCatField(customerSubCat_item: String, _oprot: TProtocol) {
    _oprot.writeFieldBegin(CustomerSubCatField)
    writeCustomerSubCatValue(customerSubCat_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeCustomerSubCatValue(customerSubCat_item: String, _oprot: TProtocol) {
    _oprot.writeString(customerSubCat_item)
  }

  private def readCustomerBalanceValue(_iprot: TProtocol): Int = {
    _iprot.readI32()
  }

  private def writeCustomerBalanceField(customerBalance_item: Int, _oprot: TProtocol) {
    _oprot.writeFieldBegin(CustomerBalanceField)
    writeCustomerBalanceValue(customerBalance_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeCustomerBalanceValue(customerBalance_item: Int, _oprot: TProtocol) {
    _oprot.writeI32(customerBalance_item)
  }

  private def readEffectiveDateValue(_iprot: TProtocol): String = {
    _iprot.readString()
  }

  private def writeEffectiveDateField(effectiveDate_item: String, _oprot: TProtocol) {
    _oprot.writeFieldBegin(EffectiveDateField)
    writeEffectiveDateValue(effectiveDate_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeEffectiveDateValue(effectiveDate_item: String, _oprot: TProtocol) {
    _oprot.writeString(effectiveDate_item)
  }



  private def ttypeToHuman(byte: Byte) = {
    // from https://github.com/apache/thrift/blob/master/lib/java/src/org/apache/thrift/protocol/TType.java
    byte match {
      case TType.STOP   => "STOP"
      case TType.VOID   => "VOID"
      case TType.BOOL   => "BOOL"
      case TType.BYTE   => "BYTE"
      case TType.DOUBLE => "DOUBLE"
      case TType.I16    => "I16"
      case TType.I32    => "I32"
      case TType.I64    => "I64"
      case TType.STRING => "STRING"
      case TType.STRUCT => "STRUCT"
      case TType.MAP    => "MAP"
      case TType.SET    => "SET"
      case TType.LIST   => "LIST"
      case TType.ENUM   => "ENUM"
      case _            => "UNKNOWN"
    }
  }

  object Immutable extends ThriftStructCodec3[Customer] {
    override def encode(_item: Customer, _oproto: TProtocol) { _item.write(_oproto) }
    override def decode(_iprot: TProtocol): Customer = Customer.decode(_iprot)
  }

  /**
   * The default read-only implementation of Customer.  You typically should not need to
   * directly reference this class; instead, use the Customer.apply method to construct
   * new instances.
   */
  class Immutable(
    val customerId: String,
    val customerName: String,
    val customerAcct: String,
    val customerCat: String,
    val customerSubCat: String,
    val customerBalance: Int,
    val effectiveDate: String,
    override val _passthroughFields: immutable$Map[Short, TFieldBlob]
  ) extends Customer {
    def this(
      customerId: String,
      customerName: String,
      customerAcct: String,
      customerCat: String,
      customerSubCat: String,
      customerBalance: Int,
      effectiveDate: String
    ) = this(
      customerId,
      customerName,
      customerAcct,
      customerCat,
      customerSubCat,
      customerBalance,
      effectiveDate,
      Map.empty
    )
  }

  /**
   * This Proxy trait allows you to extend the Customer trait with additional state or
   * behavior and implement the read-only methods from Customer using an underlying
   * instance.
   */
  trait Proxy extends Customer {
    protected def _underlying_Customer: Customer
    override def customerId: String = _underlying_Customer.customerId
    override def customerName: String = _underlying_Customer.customerName
    override def customerAcct: String = _underlying_Customer.customerAcct
    override def customerCat: String = _underlying_Customer.customerCat
    override def customerSubCat: String = _underlying_Customer.customerSubCat
    override def customerBalance: Int = _underlying_Customer.customerBalance
    override def effectiveDate: String = _underlying_Customer.effectiveDate
    override def _passthroughFields = _underlying_Customer._passthroughFields
  }
}

trait Customer
  extends ThriftStruct
  with Product7[String, String, String, String, String, Int, String]
  with java.io.Serializable
{
  import Customer._

  def customerId: String
  def customerName: String
  def customerAcct: String
  def customerCat: String
  def customerSubCat: String
  def customerBalance: Int
  def effectiveDate: String

  def _passthroughFields: immutable$Map[Short, TFieldBlob] = immutable$Map.empty

  def _1 = customerId
  def _2 = customerName
  def _3 = customerAcct
  def _4 = customerCat
  def _5 = customerSubCat
  def _6 = customerBalance
  def _7 = effectiveDate

  /**
   * Gets a field value encoded as a binary blob using TCompactProtocol.  If the specified field
   * is present in the passthrough map, that value is returend.  Otherwise, if the specified field
   * is known and not optional and set to None, then the field is serialized and returned.
   */
  def getFieldBlob(_fieldId: Short): Option[TFieldBlob] = {
    lazy val _buff = new TMemoryBuffer(32)
    lazy val _oprot = new TCompactProtocol(_buff)
    _passthroughFields.get(_fieldId) orElse {
      val _fieldOpt: Option[TField] =
        _fieldId match {
          case 1 =>
            if (customerId ne null) {
              writeCustomerIdValue(customerId, _oprot)
              Some(Customer.CustomerIdField)
            } else {
              None
            }
          case 2 =>
            if (customerName ne null) {
              writeCustomerNameValue(customerName, _oprot)
              Some(Customer.CustomerNameField)
            } else {
              None
            }
          case 3 =>
            if (customerAcct ne null) {
              writeCustomerAcctValue(customerAcct, _oprot)
              Some(Customer.CustomerAcctField)
            } else {
              None
            }
          case 4 =>
            if (customerCat ne null) {
              writeCustomerCatValue(customerCat, _oprot)
              Some(Customer.CustomerCatField)
            } else {
              None
            }
          case 5 =>
            if (customerSubCat ne null) {
              writeCustomerSubCatValue(customerSubCat, _oprot)
              Some(Customer.CustomerSubCatField)
            } else {
              None
            }
          case 6 =>
            if (true) {
              writeCustomerBalanceValue(customerBalance, _oprot)
              Some(Customer.CustomerBalanceField)
            } else {
              None
            }
          case 7 =>
            if (effectiveDate ne null) {
              writeEffectiveDateValue(effectiveDate, _oprot)
              Some(Customer.EffectiveDateField)
            } else {
              None
            }
          case _ => None
        }
      _fieldOpt match {
        case Some(_field) =>
          val _data = Arrays.copyOfRange(_buff.getArray, 0, _buff.length)
          Some(TFieldBlob(_field, _data))
        case None =>
          None
      }
    }
  }

  // /**
  //  * Collects TCompactProtocol-encoded field values according to `getFieldBlob` into a map.
  //  */
  // def getFieldBlobs(ids: TraversableOnce[Short]): immutable$Map[Short, TFieldBlob] =
  //   (ids flatMap { id => getFieldBlob(id) map { id -> _ } }).toMap

  /**
   * Sets a field using a TCompactProtocol-encoded binary blob.  If the field is a known
   * field, the blob is decoded and the field is set to the decoded value.  If the field
   * is unknown and passthrough fields are enabled, then the blob will be stored in
   * _passthroughFields.
   */
  def setField(_blob: TFieldBlob): Customer = {
    var customerId: String = this.customerId
    var customerName: String = this.customerName
    var customerAcct: String = this.customerAcct
    var customerCat: String = this.customerCat
    var customerSubCat: String = this.customerSubCat
    var customerBalance: Int = this.customerBalance
    var effectiveDate: String = this.effectiveDate
    var _passthroughFields = this._passthroughFields
    _blob.id match {
      case 1 =>
        customerId = readCustomerIdValue(_blob.read)
      case 2 =>
        customerName = readCustomerNameValue(_blob.read)
      case 3 =>
        customerAcct = readCustomerAcctValue(_blob.read)
      case 4 =>
        customerCat = readCustomerCatValue(_blob.read)
      case 5 =>
        customerSubCat = readCustomerSubCatValue(_blob.read)
      case 6 =>
        customerBalance = readCustomerBalanceValue(_blob.read)
      case 7 =>
        effectiveDate = readEffectiveDateValue(_blob.read)
      case _ => _passthroughFields += (_blob.id -> _blob)
    }
    new Immutable(
      customerId,
      customerName,
      customerAcct,
      customerCat,
      customerSubCat,
      customerBalance,
      effectiveDate,
      _passthroughFields
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is subtracked
   * from the passthroughFields map, if present.
   */
  def unsetField(_fieldId: Short): Customer = {
    var customerId: String = this.customerId
    var customerName: String = this.customerName
    var customerAcct: String = this.customerAcct
    var customerCat: String = this.customerCat
    var customerSubCat: String = this.customerSubCat
    var customerBalance: Int = this.customerBalance
    var effectiveDate: String = this.effectiveDate

    _fieldId match {
      case 1 =>
        customerId = null
      case 2 =>
        customerName = null
      case 3 =>
        customerAcct = null
      case 4 =>
        customerCat = null
      case 5 =>
        customerSubCat = null
      case 6 =>
        customerBalance = 0
      case 7 =>
        effectiveDate = null
      case _ =>
    }
    new Immutable(
      customerId,
      customerName,
      customerAcct,
      customerCat,
      customerSubCat,
      customerBalance,
      effectiveDate,
      _passthroughFields - _fieldId
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is subtracked
   * from the passthroughFields map, if present.
   */
  def unsetCustomerId: Customer = unsetField(1)

  def unsetCustomerName: Customer = unsetField(2)

  def unsetCustomerAcct: Customer = unsetField(3)

  def unsetCustomerCat: Customer = unsetField(4)

  def unsetCustomerSubCat: Customer = unsetField(5)

  def unsetCustomerBalance: Customer = unsetField(6)

  def unsetEffectiveDate: Customer = unsetField(7)


  override def write(_oprot: TProtocol) {
    Customer.validate(this)
    _oprot.writeStructBegin(Struct)
    if (customerId ne null) writeCustomerIdField(customerId, _oprot)
    if (customerName ne null) writeCustomerNameField(customerName, _oprot)
    if (customerAcct ne null) writeCustomerAcctField(customerAcct, _oprot)
    if (customerCat ne null) writeCustomerCatField(customerCat, _oprot)
    if (customerSubCat ne null) writeCustomerSubCatField(customerSubCat, _oprot)
    writeCustomerBalanceField(customerBalance, _oprot)
    if (effectiveDate ne null) writeEffectiveDateField(effectiveDate, _oprot)
    _passthroughFields.values foreach { _.write(_oprot) }
    _oprot.writeFieldStop()
    _oprot.writeStructEnd()
  }

  def copy(
    customerId: String = this.customerId,
    customerName: String = this.customerName,
    customerAcct: String = this.customerAcct,
    customerCat: String = this.customerCat,
    customerSubCat: String = this.customerSubCat,
    customerBalance: Int = this.customerBalance,
    effectiveDate: String = this.effectiveDate,
    _passthroughFields: immutable$Map[Short, TFieldBlob] = this._passthroughFields
  ): Customer =
    new Immutable(
      customerId,
      customerName,
      customerAcct,
      customerCat,
      customerSubCat,
      customerBalance,
      effectiveDate,
      _passthroughFields
    )

  override def canEqual(other: Any): Boolean = other.isInstanceOf[Customer]

  override def equals(other: Any): Boolean =
    _root_.scala.runtime.ScalaRunTime._equals(this, other) &&
      _passthroughFields == other.asInstanceOf[Customer]._passthroughFields

  override def hashCode: Int = _root_.scala.runtime.ScalaRunTime._hashCode(this)

  override def toString: String = _root_.scala.runtime.ScalaRunTime._toString(this)


  override def productArity: Int = 7

  override def productElement(n: Int): Any = n match {
    case 0 => customerId
    case 1 => customerName
    case 2 => customerAcct
    case 3 => customerCat
    case 4 => customerSubCat
    case 5 => customerBalance
    case 6 => effectiveDate
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productPrefix: String = "Customer"
}
