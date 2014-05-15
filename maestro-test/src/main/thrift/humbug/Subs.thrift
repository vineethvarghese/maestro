#@namespace scala au.com.cba.omnia.maestro.test.thrift.humbug

struct SubOne {
  1: string  stringField
  2: bool    booleanField
}

struct SubTwo {
  1: i32     intField
  2: i64     longField
  3: double  doubleField
}

struct SubThree {
  1: i64     longField
  2: i32     intField
}

struct SubFour {
  1: string field1000
  2: string field10
  3: string field100
}

struct Unsub {
  1: string otherStringField
}
