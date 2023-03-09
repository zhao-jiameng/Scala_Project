
import org.scalacheck._
import Prop._
import Test._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class Assignment3Test extends AnyFunSuite:
  test("Test 1") {
    TraceM.resetFlatMapCount()
    greatestCommonDivisor(TraceM((144, 128)))
    TraceM.getFlatMapCount shouldBe 6
  }

  test("Test 2") {
    TraceM.resetFlatMapCount()
    greatestCommonDivisor(TraceM((12345, 111111110)))
    TraceM.getFlatMapCount shouldBe 18
  }

  test("Test 3") {
    TraceM.resetFlatMapCount()
    greatestCommonDivisor(TraceM((654321777, 987654333)))
    TraceM.getFlatMapCount shouldBe 34
  }

  test("Test 4") {
    TraceM.resetFlatMapCount()
    greatestCommonDivisor(TraceM((9876543, 1234567)))
    TraceM.getFlatMapCount shouldBe 14
  }

  test("Test 5") {
    TraceM.resetFlatMapCount()
    leastCommonMultiple(TraceM((144, 128)))
    TraceM.getFlatMapCount shouldBe 26
  }

  test("Test 6") {
    TraceM.resetFlatMapCount()
    leastCommonMultiple(TraceM((111, 198)))
    TraceM.getFlatMapCount shouldBe 113
  }

  test("Test 7") {
      TraceM.resetFlatMapCount()
      leastCommonMultiple(TraceM((505, 625)))
      TraceM.getFlatMapCount shouldBe 305
  }