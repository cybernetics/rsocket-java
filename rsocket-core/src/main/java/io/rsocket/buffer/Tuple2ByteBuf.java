package io.rsocket.buffer;

import io.netty.buffer.AbstractReferenceCountedByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.agrona.BufferUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.Objects;

public class Tuple2ByteBuf extends AbstractReferenceCountedByteBuf {
  private static final int MEMORY_CACHE_ALIGNMENT = 64;

  private static final long ONE_MASK = 0x100000000L;
  private static final long TWO_MASK = 0x200000000L;
  private static final long MASK = 0x700000000L;
  private static final boolean CHECK_BOUNDS = true;
  private int capacity;
  private ByteBuf one;
  private ByteBuf two;
  private ByteBufAllocator allocator;
  private int oneReadIndex;
  private int twoReadIndex;
  private int oneReadableBytes;
  private int twoReadableBytes;
  private int twoRelativeIndex;

  Tuple2ByteBuf() {
    super(Integer.MAX_VALUE);
  }

  public static Tuple2ByteBuf create(ByteBufAllocator allocator, ByteBuf one, ByteBuf two) {
    Tuple2ByteBuf byteBuf = new Tuple2ByteBuf();
    byteBuf.wrap(allocator, one, two);
    return byteBuf;
  }

  public void wrap(ByteBufAllocator allocator, ByteBuf one, ByteBuf two) {
    Objects.requireNonNull(allocator);
    Objects.requireNonNull(one);
    Objects.requireNonNull(two);

    this.allocator = allocator;
    this.one = one;
    this.two = two;

    this.oneReadIndex = one.readerIndex();
    this.twoReadIndex = two.readerIndex();

    this.oneReadableBytes = one.readableBytes();
    this.twoReadableBytes = two.readableBytes();

    this.twoRelativeIndex = oneReadableBytes;

    this.capacity = oneReadableBytes + twoReadableBytes;

    super.writerIndex(capacity);
  }

  public long calculateRelativeIndex(int index) {
    if (CHECK_BOUNDS && index > capacity) {
      throw new IndexOutOfBoundsException(
          "index " + index + " is out of bounds, capacity is " + capacity);
    }
    long relativeIndex;
    long mask;
    if (index >= twoRelativeIndex) {
      relativeIndex = twoReadIndex + (index - oneReadableBytes);
      mask = TWO_MASK;
    } else {
      relativeIndex = oneReadIndex + index;
      mask = ONE_MASK;
    }

    return relativeIndex | mask;
  }

  @Override
  public int capacity() {
    return capacity;
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int maxCapacity() {
    return capacity;
  }

  @Override
  public ByteBufAllocator alloc() {
    return allocator;
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.BIG_ENDIAN;
  }

  @Override
  public ByteBuf order(ByteOrder endianness) {
    return this;
  }

  @Override
  public ByteBuf unwrap() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDirect() {
    return one.isDirect() && two.isDirect();
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public ByteBuf asReadOnly() {
    return this;
  }

  @Override
  public ByteBuf readerIndex(int readerIndex) {
    super.readerIndex(readerIndex);
    return this;
  }

  @Override
  public int writerIndex() {
    return capacity;
  }

  @Override
  public ByteBuf writerIndex(int writerIndex) {
    return this;
  }

  @Override
  public ByteBuf setIndex(int readerIndex, int writerIndex) {
    return this;
  }

  @Override
  public int writableBytes() {
    return 0;
  }

  @Override
  public int maxWritableBytes() {
    return 0;
  }

  @Override
  public boolean isWritable() {
    return false;
  }

  @Override
  public boolean isWritable(int size) {
    return false;
  }

  @Override
  public ByteBuf clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf markWriterIndex() {
    return this;
  }

  @Override
  public ByteBuf resetWriterIndex() {
    return this;
  }

  @Override
  public ByteBuf discardReadBytes() {
    return this;
  }

  @Override
  public ByteBuf discardSomeReadBytes() {
    return this;
  }

  @Override
  public ByteBuf ensureWritable(int minWritableBytes) {
    return this;
  }

  @Override
  public int ensureWritable(int minWritableBytes, boolean force) {
    return 0;
  }

  @Override
  public ByteBuf setBoolean(int index, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setShortLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setMedium(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setMediumLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setIntLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setLongLE(int index, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setChar(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setFloat(int index, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setDouble(int index, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setBytes(int index, FileChannel in, long position, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setZero(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBoolean(boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeByte(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeShort(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeShortLE(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeMedium(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeMediumLE(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeInt(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeIntLE(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeLong(long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeLongLE(long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeChar(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeFloat(float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeDouble(double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(byte[] src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuffer src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeBytes(InputStream in, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeBytes(ScatteringByteChannel in, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeBytes(FileChannel in, long position, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeZero(int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeCharSequence(CharSequence sequence, Charset charset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int nioBufferCount() {
    return one.nioBufferCount() + two.nioBufferCount();
  }

  @Override
  public ByteBuffer nioBuffer() {

    ByteBuffer[] oneBuffers = one.nioBuffers();
    ByteBuffer[] twoBuffers = two.nioBuffers();

    ByteBuffer merged = BufferUtil.allocateDirectAligned(capacity, MEMORY_CACHE_ALIGNMENT);

    for (ByteBuffer b : oneBuffers) {
      merged.put(b);
    }

    for (ByteBuffer b : twoBuffers) {
      merged.put(b);
    }

    merged.flip();
    return merged;
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    // TODO - make this smarter
    return ((ByteBuffer) nioBuffer().position(index).limit(length)).slice();
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer[] nioBuffers() {
    ByteBuffer[] oneBuffers = one.nioBuffers();
    ByteBuffer[] twoBuffers = two.nioBuffers();

    int oneLength = oneBuffers.length;
    int twoLength = twoBuffers.length;

    ByteBuffer[] buffers = new ByteBuffer[+twoBuffers.length];

    System.arraycopy(oneBuffers, 0, buffers, 0, oneLength);
    System.arraycopy(twoBuffers, 0, buffers, oneLength, twoLength);

    return buffers;
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return new ByteBuffer[0];
  }

  @Override
  public boolean hasArray() {
    return false;
  }

  @Override
  public byte[] array() {
    return new byte[0];
  }

  @Override
  public int arrayOffset() {
    return one.arrayOffset();
  }

  @Override
  public boolean hasMemoryAddress() {
    return false;
  }

  @Override
  public long memoryAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString(Charset charset) {
    StringBuilder builder = new StringBuilder(3);
    builder.append(one.toString(charset));
    builder.append(two.toString(charset));
    return builder.toString();
  }

  @Override
  public String toString(int index, int length, Charset charset) {
    // TODO - make this smarter
    return toString(charset).substring(index, length);
  }

  @Override
  public int compareTo(ByteBuf buffer) {
    return 0;
  }

  /// Override

  @Override
  protected void deallocate() {
    ReferenceCountUtil.safeRelease(one);
    ReferenceCountUtil.safeRelease(two);
  }

  @Override
  protected byte _getByte(int index) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        return one.getByte(index);
      case 0x2:
        return two.getByte(index);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected short _getShort(int index) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        return one.getShort(index);
      case 0x2:
        return two.getShort(index);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected short _getShortLE(int index) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        return one.getShort(index);
      case 0x2:
        return two.getShort(index);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        return one.getUnsignedMedium(index);
      case 0x2:
        return two.getUnsignedMedium(index);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected int _getUnsignedMediumLE(int index) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        return one.getUnsignedMediumLE(index);
      case 0x2:
        return two.getUnsignedMediumLE(index);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected int _getInt(int index) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        return one.getInt(index);
      case 0x2:
        return two.getInt(index);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected int _getIntLE(int index) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        return one.getIntLE(index);
      case 0x2:
        return two.getIntLE(index);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected long _getLong(int index) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        return one.getLong(index);
      case 0x2:
        return two.getLong(index);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected long _getLongLE(int index) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        return one.getLongLE(index);
      case 0x2:
        return two.getLongLE(index);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected void _setByte(int index, int value) {}

  @Override
  protected void _setShort(int index, int value) {}

  @Override
  protected void _setShortLE(int index, int value) {}

  @Override
  protected void _setMedium(int index, int value) {}

  @Override
  protected void _setMediumLE(int index, int value) {}

  @Override
  protected void _setInt(int index, int value) {}

  @Override
  protected void _setIntLE(int index, int value) {}

  @Override
  protected void _setLong(int index, long value) {}

  @Override
  protected void _setLongLE(int index, long value) {}

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        {
          int l = Math.min(oneReadableBytes, length);
          one.getBytes(index, dst, dstIndex, l);

          if (oneReadableBytes - l != 0) {
            l = Math.min(twoReadableBytes, length);
            two.getBytes(twoReadIndex, dst, dstIndex, l);
          }
          break;
        }
      case 0x2:
        {
          int l = Math.min(twoReadableBytes, length);
          two.getBytes(index, dst, dstIndex, l);
          break;
        }
      default:
        throw new IllegalStateException();
    }

    return this;
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    ByteBuf dstBuf = Unpooled.wrappedBuffer(dst);
    int min = Math.min(dst.length, capacity);
    return getBytes(0, dstBuf, index, min);
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    ByteBuf dstBuf = Unpooled.wrappedBuffer(dst);
    int min = Math.min(dst.limit(), capacity);
    return getBytes(0, dstBuf, index, min);
  }

  @Override
  public ByteBuf getBytes(int index, final OutputStream out, int length) throws IOException {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        {
          int l = Math.min(oneReadableBytes, length);
          one.getBytes(index, out, l);
          length -= l;

          if (oneReadableBytes - l != 0) {
            l = Math.min(twoReadableBytes, length);
            two.getBytes(twoReadIndex, out, l);
          }
          break;
        }
      case 0x2:
        {
          int l = Math.min(twoReadableBytes, length);
          two.getBytes(index, out, l);
          break;
        }
      default:
        throw new IllegalStateException();
    }

    return this;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    int read = 0;
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        {
          int l = Math.min(oneReadableBytes, length);
          read += one.getBytes(index, out, l);
          length -= l;

          if (oneReadableBytes - l != 0) {
            l = Math.min(twoReadableBytes, length);
            read += two.getBytes(twoReadIndex, out, l);
          }
          break;
        }
      case 0x2:
        {
          int l = Math.min(twoReadableBytes, length);
          read += two.getBytes(index, out, l);
          break;
        }
      default:
        throw new IllegalStateException();
    }

    return read;
  }

  @Override
  public int getBytes(int index, FileChannel out, long position, int length) throws IOException {
    int read = 0;
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        {
          int l = Math.min(oneReadableBytes, length);
          read += one.getBytes(index, out, position, l);
          length -= l;

          if (oneReadableBytes - l != 0) {
            l = Math.min(twoReadableBytes, length);
            read += two.getBytes(twoReadIndex, out, position, l);
          }
          break;
        }
      case 0x2:
        {
          int l = Math.min(twoReadableBytes, length);
          read += two.getBytes(index, out, position, l);
          break;
        }
      default:
        throw new IllegalStateException();
    }

    return read;
  }

  @Override
  public ByteBuf copy(int index, int length) {
    ByteBuf buffer = allocator.buffer();

    buffer.setBytes(oneReadIndex, one);
    buffer.setBytes(twoReadIndex, two);

    return buffer;
  }

  @Override
  public ByteBuf slice(final int readIndex, int length) {
    if (length + readIndex > capacity) {
      throw new IndexOutOfBoundsException(
          String.format(
              "readerIndex(%d) + length(%d) exceeds writerIndex(%d): %s",
              readIndex, length, capacity, this));
    }
    
    if (readIndex == 0 && length == capacity) {
      return Tuple2ByteBuf.create(
          allocator,
          one.slice(oneReadIndex, oneReadableBytes),
          two.slice(twoReadIndex, twoReadableBytes));
    }

    long ri = calculateRelativeIndex(readIndex);
    int index = (int) (ri & Integer.MAX_VALUE);

    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        {
          ByteBuf oneSlice;
          ByteBuf twoSlice;

          int l = Math.min(oneReadableBytes - index, length);

          if (length > oneReadableBytes - index) {
            oneSlice = one;
            length -= l;
            l = Math.min(twoReadableBytes, length);
            twoSlice = two.slice(twoReadIndex, l);
            return Tuple2ByteBuf.create(allocator, oneSlice, twoSlice);
          } else {
            return one.slice(index, l);
          }
        }
      case 0x2:
        {
          return two.slice(index, length);
        }
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public String toString() {
    return "Tuple2ByteBuf{"
        + "capacity="
        + capacity
        + ", one="
        + one
        + ", two="
        + two
        + ", allocator="
        + allocator
        + ", oneReadIndex="
        + oneReadIndex
        + ", twoReadIndex="
        + twoReadIndex
        + ", oneReadableBytes="
        + oneReadableBytes
        + ", twoReadableBytes="
        + twoReadableBytes
        + ", twoRelativeIndex="
        + twoRelativeIndex
        + '}';
  }
}
