/*
 * Copyright 2019 http4s.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.http4s.ember.core.h2

import cats.syntax.all._
import munit.CatsEffectSuite
import scodec.bits._

class H2FrameSpec extends CatsEffectSuite {

  test("RawFrame Should Traverse") {
    val init = H2Frame.RawFrame(
      2,
      H2Frame.Settings.`type`,
      0x01,
      Int.MaxValue,
      ByteVector(0x00, 0x02),
    )
    val bv = H2Frame.RawFrame.toByteVector(init)
    val parsed = H2Frame.RawFrame.fromByteVector(bv).map(_._1)

    assertEquals(parsed, init.some)
  }

  test("RawFrame should return excess data") {
    val init = H2Frame.RawFrame(
      2,
      H2Frame.Settings.`type`,
      0x01,
      Int.MaxValue,
      ByteVector(0x00, 0x02, 0x04),
    )
    val bv = H2Frame.RawFrame.toByteVector(init)
    val parsed = H2Frame.RawFrame.fromByteVector(bv).map(_._2)

    assertEquals(parsed, ByteVector(0x04).some)
  }

  test("Decode A Data H2Frame from Raw") {
    val init = H2Frame.RawFrame(
      2,
      H2Frame.Data.`type`,
      0x01,
      Int.MaxValue,
      ByteVector(0x00, 0x02),
    )
    val bv = H2Frame.RawFrame.toByteVector(init)
    val intermediate = H2Frame.RawFrame.fromByteVector(bv).map(_._1)
    val parsed = intermediate.flatMap(H2Frame.Data.fromRaw(_).toOption)
    val expected = H2Frame.Data(Int.MaxValue, ByteVector(0x00, 0x02), None, endStream = true)

    assertEquals(parsed, expected.some)
  }

  test("Data H2Frame should traverse") {
    val init = H2Frame.Data(4, ByteVector(0x02, 0xa0), None, endStream = false)
    val encoded = H2Frame.Data.toRaw(init)
    val back = H2Frame.Data.fromRaw(encoded)
    assertEquals(back, init.asRight)
  }

  test("Data H2Frame should traverse with padding") {
    val init =
      H2Frame.Data(
        4,
        ByteVector(0x02, 0xa0),
        Some(ByteVector(0xff, 0xff, 0xff, 0xff)),
        endStream = false,
      )
    val encoded = H2Frame.Data.toRaw(init)
    val back = H2Frame.Data.fromRaw(encoded)
    assertEquals(back, init.asRight)
  }

  test("Headers should traverse") {
    val init = H2Frame.Headers(
      7,
      Some(H2Frame.Headers.StreamDependency(exclusive = true, 4, 3)),
      endStream = true,
      endHeaders = true,
      ByteVector(3, 4),
      Some(ByteVector(1)),
    )
    val encoded = H2Frame.Headers.toRaw(init)
    val back = H2Frame.Headers.fromRaw(encoded)
    assertEquals(
      back,
      init.asRight,
    )
  }

  test("Priority should traverse") {
    val init = H2Frame.Priority(5, exclusive = true, 7, 0x01)
    val encoded = H2Frame.Priority.toRaw(init)
    val back = H2Frame.Priority.fromRaw(encoded)
    assertEquals(
      back,
      init.asRight,
    )
  }

  test("RstStream should traverse") {
    val init = H2Frame.RstStream(4, 73)
    val encoded = H2Frame.RstStream.toRaw(init)
    val back = H2Frame.RstStream.fromRaw(encoded)
    assertEquals(
      back,
      init.asRight,
    )
  }

  test("Settings should traverse") {
    val init = H2Frame.Settings(
      0x0,
      ack = false,
      List(
        H2Frame.Settings.SettingsHeaderTableSize(4096),
        H2Frame.Settings.SettingsEnablePush(isEnabled = true),
        H2Frame.Settings.SettingsMaxConcurrentStreams(1024),
        H2Frame.Settings.SettingsInitialWindowSize(65535),
        H2Frame.Settings.SettingsMaxFrameSize(16384),
        H2Frame.Settings.SettingsMaxHeaderListSize(4096),
        H2Frame.Settings.Unknown(14, 56),
      ),
    )
    val encoded = H2Frame.Settings.toRaw(init)
    val back = H2Frame.Settings.fromRaw(encoded)
    assertEquals(
      back,
      init.asRight,
    )
  }

  test("Settings should accept the maximum number of entries") {
    val payload = ByteVector.concat(
      List.fill(32)(ByteVector(0xff, 0xff, 0x00, 0x00, 0x00, 0x00))
    )
    val parsed = H2Frame.Settings.fromPayload(payload, 0x0, ack = false)

    assertEquals(parsed.map(_.list.length), 32.asRight)
  }

  test("Settings should reject more entries than the maximum") {
    val payload = ByteVector.concat(
      List.fill(33)(ByteVector(0xff, 0xff, 0x00, 0x00, 0x00, 0x00))
    )
    val parsed = H2Frame.Settings.fromPayload(payload, 0x0, ack = false)

    assertEquals(parsed, H2Error.EnhanceYourCalm.asLeft)
  }

  test("Settings should reject a dense frame within the maximum frame size") {
    val maxFrameSize = H2Frame.Settings.ConnectionSettings.default.maxFrameSize.frameSize
    val payload = ByteVector.concat(
      List.fill(maxFrameSize / 6)(ByteVector(0xff, 0xff, 0x00, 0x00, 0x00, 0x00))
    )
    val raw = H2Frame.RawFrame(
      payload.size.toInt,
      H2Frame.Settings.`type`,
      0x00,
      0x0,
      payload,
    )

    assert(payload.size <= maxFrameSize)
    assertEquals(H2Frame.fromRaw(raw), H2Error.EnhanceYourCalm.asLeft)
  }

  test("PushPromise should traverse") {
    val init =
      H2Frame.PushPromise(
        74,
        endHeaders = true,
        107,
        ByteVector(0x1, 0xe, 0xb),
        Some(ByteVector(0x0, 0x0, 0x0)),
      )
    val encoded = H2Frame.PushPromise.toRaw(init)
    val back = H2Frame.PushPromise.fromRaw(encoded).toOption
    assertEquals(
      back,
      init.some,
    )
  }

  test("Ping should traverse") {
    val init = H2Frame.Ping.ack
    val encoded = H2Frame.Ping.toRaw(init)
    val back = H2Frame.Ping.fromRaw(encoded).toOption
    assertEquals(
      back,
      init.some,
    )
  }

  test("GoAway should traverse") {
    val init = H2Frame.GoAway(0, 476, Int.MaxValue, Some(ByteVector(0xa)))
    val encoded = H2Frame.GoAway.toRaw(init)
    val back = H2Frame.GoAway.fromRaw(encoded).toOption
    assertEquals(
      back,
      init.some,
    )
  }

  test("WindowUpdate should traverse") {
    val init = H2Frame.WindowUpdate(73, 10210)
    val encoded = H2Frame.WindowUpdate.toRaw(init)
    val back = H2Frame.WindowUpdate.fromRaw(encoded).toOption
    assertEquals(
      back,
      init.some,
    )
  }

  test("Continuation should traverse") {
    val init = H2Frame.Continuation(73, endHeaders = true, ByteVector(0xe, 0x8, 0x3))
    val encoded = H2Frame.Continuation.toRaw(init)
    val back = H2Frame.Continuation.fromRaw(encoded).toOption
    assertEquals(
      back,
      init.some,
    )
  }

  test("all short known frame payloads fail safely or decode without exceptions") {
    for {
      kind <- 0 to 9
      flags <- 0 to 255
      size <- 0 to 12
    } {
      val id = if (Set(4, 6, 7).contains(kind)) 0 else 1
      H2Frame.fromRaw(
        H2Frame.RawFrame(size, kind.toByte, flags.toByte, id, ByteVector.fill(size.toLong)(0))
      )
    }
  }

  test("stream and connection frames reject the wrong stream identifier") {
    val streamFrames: List[H2Frame] = List(
      H2Frame.Data(0, ByteVector.empty, None, false),
      H2Frame.Headers(0, None, false, true, ByteVector.empty, None),
      H2Frame.Priority(0, false, 1, 1),
      H2Frame.RstStream(0, 0),
      H2Frame.PushPromise(0, true, 2, ByteVector.empty, None),
      H2Frame.Continuation(0, true, ByteVector.empty),
      H2Frame.Ping.default.copy(identifier = 1),
      H2Frame.Settings.Ack.copy(identifier = 1),
      H2Frame.GoAway(1, 0, 0, None),
    )
    streamFrames.foreach(f => assertEquals(H2Frame.fromRaw(f.toRaw), Left(H2Error.ProtocolError)))
  }

  test("padding lengths are unsigned octets for DATA, HEADERS and PUSH_PROMISE") {
    List(0, 127, 128, 255).foreach { size =>
      val pad = Some(ByteVector.fill(size.toLong)(0))
      val frames: List[H2Frame] = List(
        H2Frame.Data(1, ByteVector(42), pad, false),
        H2Frame.Headers(1, None, false, true, ByteVector(42), pad),
        H2Frame.Headers(
          1,
          Some(H2Frame.Headers.StreamDependency(true, 0x7f80ff80, 1)),
          false,
          true,
          ByteVector(42),
          pad,
        ),
        H2Frame.PushPromise(1, true, 2, ByteVector(42), pad),
      )
      frames.foreach(f => assertEquals(H2Frame.fromRaw(f.toRaw), Right(f)))
    }
  }

  test("padding cannot overlap mandatory fields or exceed the payload") {
    List(
      (0, 8, ByteVector(2, 0)),
      (1, 8, ByteVector(2, 0)),
      (1, 40, ByteVector(1, 0, 0, 0, 0, 1)),
      (5, 8, ByteVector(1, 0, 0, 0, 2)),
    ).foreach { case (kind, flags, bytes) =>
      assertEquals(
        H2Frame.fromRaw(H2Frame.RawFrame(bytes.size.toInt, kind.toByte, flags.toByte, 1, bytes)),
        Left(H2Error.ProtocolError),
      )
    }
  }

  test("priority dependencies retain unsigned low octets") {
    val frame = H2Frame.Priority(1, true, 0x7f80ff80, 1)
    assertEquals(H2Frame.fromRaw(frame.toRaw), Right(frame))
  }

  test("maximum initial window is 2^31 - 1") {
    assertEquals(
      H2Frame.Settings.SettingsInitialWindowSize.fromInt(Int.MaxValue),
      Right(H2Frame.Settings.SettingsInitialWindowSize(Int.MaxValue)),
    )
  }

}
