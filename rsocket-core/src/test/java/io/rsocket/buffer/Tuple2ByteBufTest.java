package io.rsocket.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.junit.jupiter.api.Test;

class Tuple2ByteBufTest {
  ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
  @Test
  public void test() {
    ByteBuf one = ByteBufUtil.writeUtf8(allocator, "_string one_");
    ByteBuf two = ByteBufUtil.writeUtf8(allocator, "_string two_");
    ByteBuf byteBuf = Tuple2ByteBuf.create(allocator, one, two);
    String s = ByteBufUtil.prettyHexDump(byteBuf);
    System.out.println(s);
    short unsignedByte = byteBuf.getUnsignedByte(12);
    System.out.println(unsignedByte);
    ByteBuf slice = byteBuf.slice(12, 12);
    String prettyHexDump = ByteBufUtil.prettyHexDump(slice);
    System.out.println(prettyHexDump);
  }
}