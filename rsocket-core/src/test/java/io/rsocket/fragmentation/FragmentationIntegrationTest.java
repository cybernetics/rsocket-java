package io.rsocket.fragmentation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameUtil;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.util.DefaultPayload;
import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.concurrent.ThreadLocalRandom;

public class FragmentationIntegrationTest {
  private static byte[] data = new byte[128];
  private static byte[] metadata = new byte[128];
  
  static {
    ThreadLocalRandom.current().nextBytes(data);
    ThreadLocalRandom.current().nextBytes(metadata);
  }
  
  private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
  
  @DisplayName("fragments and reassmbles data")
  @Test
  void fragmentAndReassembleData() {
    ByteBuf frame = PayloadFrameFlyweight
                        .encodeNextComplete(allocator, 2, DefaultPayload.create(data));
  
    Publisher<ByteBuf> fragments = FrameFragmenter.fragmentFrame(allocator, 64, frame, FrameHeaderFlyweight.frameType(frame), false);
    
    FrameReassembler reassembler = new FrameReassembler(allocator);

    ByteBuf assembled =
        Flux.from(fragments)
            .doOnNext(byteBuf -> System.out.println(FrameUtil.toString(byteBuf)))
            .handle(reassembler::reassembleFrame)
            .blockLast();
  
    String s = FrameUtil.toString(assembled);
    System.out.println(s);

    Assert.assertEquals(frame, assembled);
  }
}
