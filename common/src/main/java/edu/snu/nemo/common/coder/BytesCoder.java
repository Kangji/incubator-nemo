/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.common.coder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * A {@link Coder} which is used for an array of bytes.
 */
public final class BytesCoder implements Coder<byte[]> {
  private static final Logger LOG = LoggerFactory.getLogger(BytesCoder.class.getName());

  /**
   * Constructor.
   */
  public BytesCoder() {
  }

  /**
   * The array of bytes to write has to have the exact length to write.
   * @see Coder#encode(Object, OutputStream).
   */
  @Override
  public void encode(final byte[] value, final OutputStream outStream) throws IOException {
    outStream.write(value);
  }

  @Override
  public byte[] decode(final InputStream inStream) throws IOException {
    //LOG.warn("InputStream#available() might not exactly represent the number of bytes to read. "
    //    + "Please use decode(int, InputStream) method with explicit length.");
    //return this.decode(inStream.available(), inStream);
    final int lengthToRead = inStream.available();
    final byte[] bytes = new byte[lengthToRead]; // Read the size of this byte array.
    final int readBytes = inStream.read(bytes);
    if (lengthToRead != readBytes) {
      throw new IOException("Have to read " + lengthToRead + " but read only " + readBytes + " bytes.");
    }
    return bytes;
  }

  /**
   * Decodes the a value from the given input stream.
   * It have to be able to decode the given stream consequently by calling this method repeatedly.
   * Because there are many elements in the input stream, the stream should not be closed.
   *
   * @param lengthToRead the length to read.
   * @param inStream     the stream from which bytes are read.
   * @return the decoded bytes.
   * @throws IOException if fail to decode
   */
  /*public byte[] decode(final int lengthToRead,
                       final InputStream inStream) throws IOException {
    final byte[] bytes = new byte[lengthToRead]; // Read the size of this byte array.
    final int readBytes = inStream.read(bytes);
    if (lengthToRead != readBytes) {
      throw new IOException("Have to read " + lengthToRead + " but read only " + readBytes + " bytes.");
    }
    return bytes;
  }*/
}
