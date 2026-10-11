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

import cats.Applicative
import fs2.Chunk

/** Work evaluated by the single connection writer immediately before transmission. */
private[h2] final case class H2Write[F[_]](frames: F[Chunk[H2Frame]])

private[h2] object H2Write {
  def now[F[_]: Applicative](frames: Chunk[H2Frame]): H2Write[F] =
    H2Write(Applicative[F].pure(frames))

  def one[F[_]: Applicative](frame: H2Frame): H2Write[F] =
    now(Chunk.singleton(frame))
}
