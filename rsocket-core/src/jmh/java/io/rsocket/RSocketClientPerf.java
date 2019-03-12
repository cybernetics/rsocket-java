package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.util.ByteBufPayload;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@BenchmarkMode(Mode.Throughput)
@Fork(
    value = 1,
    jvmArgsAppend = {
      "-Dio.netty.buffer.checkBounds=false",
      "-Dio.netty.buffer.checkAccessible=false",
      "-Dio.netty.leakDetectionLevel=disabled",
      "-Dio.netty.allocator.directMemoryCacheAlignment=64",
      "-Dio.netty.leakDetection.level=disabled",
      "-XX:+UnlockCommercialFeatures",
      "-XX:+FlightRecorder",
    })
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Thread)
public class RSocketClientPerf {
  @Benchmark
  public void requestResponse(RSocketServerPerf.Input input) throws Exception {

    Thread t = Thread.currentThread();

    Schedulers.parallel()
        .schedule(
            () -> {
              DuplexConnection duplexConnection =
                  new DuplexConnection() {
                    DirectProcessor<ByteBuf> streamIds = DirectProcessor.create();

                    @Override
                    public Mono<Void> send(Publisher<ByteBuf> frames) {
                      return Flux.from(frames)
                          .doOnNext(
                              byteBuf -> {
                                byteBuf.release();
                                ByteBuf encode =
                                    PayloadFrameFlyweight.encodeNextComplete(
                                        ByteBufAllocator.DEFAULT,
                                        FrameHeaderFlyweight.streamId(byteBuf),
                                        ByteBufPayload.create(Unpooled.wrappedBuffer(input.data)));
                                streamIds.onNext(encode);
                              })
                          .then();
                    }

                    @Override
                    public Flux<ByteBuf> receive() {
                      return streamIds;
                    }

                    @Override
                    public Mono<Void> onClose() {
                      return Mono.never();
                    }

                    @Override
                    public void dispose() {}
                  };

              RSocketClient client =
                  new RSocketClient(
                      ByteBufAllocator.DEFAULT,
                      duplexConnection,
                      PayloadDecoder.ZERO_COPY,
                      Throwable::printStackTrace,
                      StreamIdSupplier.clientSupplier());

              Flux.range(1, 100_000)
                  .flatMap(
                      i -> {
                        return client.requestResponse(
                            ByteBufPayload.create(Unpooled.wrappedBuffer(input.data)));
                      })
                  .subscribe(
                      Payload::release, Throwable::printStackTrace, () -> LockSupport.unpark(t));
            });

    LockSupport.park();
  }

  @State(Scope.Benchmark)
  public static class Input {
    Payload pong;
    Payload payload;
    Blackhole bh;

    byte[] data = new byte[64];

    @Setup
    public void setup(Blackhole bh) {
      this.bh = bh;
      this.payload = ByteBufPayload.create("hello");
      ThreadLocalRandom.current().nextBytes(data);
      pong = ByteBufPayload.create(data);
    }
  }
}
