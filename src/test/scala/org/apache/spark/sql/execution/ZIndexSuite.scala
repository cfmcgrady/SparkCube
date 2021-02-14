/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{HiveHash, Literal}

class ZIndexSuite extends SparkFunSuite{

  test("array zindex base suite") {
    val d1 = ArrayZIndex(Array(1000))
    val d2 = ArrayZIndex(Array(1001))
    assert(d1.compare(d2) == -1)
    assert(d2.compare(d1) == 1)
    assert(d2.compare(d2) == 0)

    val d21 = ArrayZIndex(Array(3, 1))
    val d22 = ArrayZIndex(Array(3, 2))
    assert(d21.compare(d22) == -1)

    val d31 = ArrayZIndex(Array(3, 3))
    val d32 = ArrayZIndex(Array(3, 7))
    assert(d31.compare(d32) == -1)

    val d41 = ArrayZIndex(Array(3, 3))
    val d42 = ArrayZIndex(Array(2, 7))
    assert(d41.compare(d42) == -1)

    assert(
      ArrayZIndex(Array(Int.MaxValue)).toBinaryString == "0" + Integer.toBinaryString(Int.MaxValue)
    )
  }

  test("array zindex v2 base suite") {

    val d1 = ArrayZIndexV2.create(Array(1000))
    val d2 = ArrayZIndexV2.create(Array(1001))
    assert(d1.compare(d2) == -1)
    assert(d2.compare(d1) == 1)
    assert(d2.compare(d2) == 0)

    val d21 = ArrayZIndexV2.create(Array(3, 1))
    val d22 = ArrayZIndexV2.create(Array(3, 2))
    assert(d21.compare(d22) == -1)

    val d31 = ArrayZIndexV2.create(Array(3, 3))
    val d32 = ArrayZIndexV2.create(Array(3, 7))
    assert(d31.compare(d32) == -1)

    val d41 = ArrayZIndexV2.create(Array(3, 3))
    val d42 = ArrayZIndexV2.create(Array(2, 7))
    assert(d41.compare(d42) == -1)

    assert(
      ArrayZIndexV2.create(Array(Int.MaxValue)).toBinaryString ==
        "0" + Integer.toBinaryString(Int.MaxValue)
    )
  }

  test("binary op") {

    println(Integer.toBinaryString(8))
    println((1 << 31) - 1)
    println(Int.MaxValue)
    println(Integer.toBinaryString((1 << 3) - 1))
    println(Integer.toBinaryString(8 - 1).length)
//    println(Integer.toBinaryString(-2))
    println("/tmp/aa/dd".split("/").last)


  }

}
