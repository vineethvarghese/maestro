package com.cba.omnia.maestro.benchmark
package codec

import com.cba.omnia.maestro.benchmark.thrift._
import com.cba.omnia.maestro.core.codec._
import com.cba.omnia.maestro.macros._

import scalaz._, Scalaz._

import org.scalameter.api._


object Data extends java.io.Serializable {
  implicit val BasicDecode: Decode[Basic] =
    Macros.mkDecode[Basic]

  implicit val BasicEncode: Encode[Basic] =
    Macros.mkEncode[Basic]

  def data(n: Int): List[Basic] = (1 to n).toList.map(n =>
    Basic("basic-" + n, n, Int.MaxValue.toLong + n.toLong)
  )

  def source(n: Int): List[DecodeSource] =
    data(n).map(d => ValDecodeSource(Encode.encode(d)))
}

object CodecBenchmark extends PerformanceTest.Microbenchmark {
  import Data._

  val sizes = Gen.range("size")(10000, 20000, 10000)

  val toEncode = sizes.map(data(_))

  val toDecode = sizes.map(source(_))

  performance of "Codecs" in {
    measure method "encode" in {
      using(toEncode) in { e =>
        e.map(b => Encode.encode(b))
      }
    }

    measure method "decode" in {
      using(toDecode) in { d =>
        d.map(Decode.decode[Basic])
      }
    }
  }
}
