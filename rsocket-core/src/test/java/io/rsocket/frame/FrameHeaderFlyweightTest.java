package io.rsocket.frame;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;

class FrameHeaderFlyweightTest {
  // Taken from spec
  private static final int FRAME_MAX_SIZE = 16_777_215;

  @Test
  void typeAndFlag() {
    FrameType frameType = FrameType.REQUEST_FNF;
    int flags = 0b1110110111;
    ByteBuf header = FrameHeaderFlyweight.encode(ByteBufAllocator.DEFAULT, 0, frameType, flags);

    assertEquals(flags, FrameHeaderFlyweight.flags(header));
    assertEquals(frameType, FrameHeaderFlyweight.frameType(header));
    header.release();
  }

  @Test
  void typeAndFlagTruncated() {
    FrameType frameType = FrameType.SETUP;
    int flags = 0b11110110111; // 1 bit too many
    ByteBuf header = FrameHeaderFlyweight.encode(ByteBufAllocator.DEFAULT, 0, frameType, flags);

    assertNotEquals(flags, FrameHeaderFlyweight.flags(header));
    assertEquals(flags & 0b0000_0011_1111_1111, FrameHeaderFlyweight.flags(header));
    assertEquals(frameType, FrameHeaderFlyweight.frameType(header));
    header.release();
  }
  
  @Test
  void replaceStreamId() {
    ByteBuf frame = FrameHeaderFlyweight
                       .encode(ByteBufAllocator.DEFAULT, 123, FrameType.SETUP, 0);
    int old = FrameHeaderFlyweight.replaceStreamId(frame, 321);
    assertEquals(123, old);
    assertEquals(321, FrameHeaderFlyweight.streamId(frame));
    frame.release();
  }
  
  @Test
  void removeAndAppendStreamId() {
    ByteBuf frame = FrameHeaderFlyweight
                      .encode(ByteBufAllocator.DEFAULT, 123, FrameType.SETUP, 0);
  
    ByteBuf removed = FrameHeaderFlyweight
                        .removeStreamId(frame);
  
    ByteBuf append = FrameHeaderFlyweight.appendStreamId(ByteBufAllocator.DEFAULT, removed, 321);
  
    assertEquals(321, FrameHeaderFlyweight.streamId(append));
    append.release();
  }
}
