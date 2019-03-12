package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.frame.RequestResponseFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.util.ByteBufPayload;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
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
public class RSocketServerPerf {
  @Benchmark
  public void requestResponse(Input input) throws Exception {
    AtomicLong count = new AtomicLong();

    Thread t = Thread.currentThread();

    Schedulers.parallel()
        .schedule(
            () -> {
              DuplexConnection duplexConnection =
                  new DuplexConnection() {
                    StreamIdSupplier supplier = StreamIdSupplier.clientSupplier();

                    @Override
                    public Mono<Void> send(Publisher<ByteBuf> frames) {
                      return Flux.from(frames)
                          .doOnNext(
                              byteBuf -> {
                                byteBuf.release();
                                count.incrementAndGet();
                              })
                          .then();
                    }

                    @Override
                    public Flux<ByteBuf> receive() {
                      return Flux.generate(
                          sink -> {
                            if (count.get() >= 100_000) {
                              sink.complete();
                              LockSupport.unpark(t);
                            }
                            try {
                              ByteBuf encode =
                                  RequestResponseFrameFlyweight.encode(
                                      ByteBufAllocator.DEFAULT,
                                      supplier.nextStreamId(),
                                      false,
                                      null,
                                      Unpooled.wrappedBuffer(input.data));
                              sink.next(encode);
                            } catch (Throwable t) {
                              sink.error(t);
                            }
                          });
                    }

                    @Override
                    public Mono<Void> onClose() {
                      return Mono.never();
                    }

                    @Override
                    public void dispose() {}
                  };

              RSocketServer server =
                  new RSocketServer(
                      ByteBufAllocator.DEFAULT,
                      duplexConnection,
                      new AbstractRSocket() {
                        @Override
                        public Mono<Payload> requestResponse(Payload payload) {
                          payload.release();
                          return Mono.just(input.pong.retain());
                        }
                      },
                      PayloadDecoder.ZERO_COPY,
                      Throwable::printStackTrace);
            });

    LockSupport.park();
  }

  @State(Scope.Benchmark)
  public static class Input {
    Payload pong;
    Payload payload;
    Blackhole bh;

    byte[] data = new byte[1 << 18];

    @Setup
    public void setup(Blackhole bh) {
      this.bh = bh;
      this.payload = ByteBufPayload.create("hello");
      ThreadLocalRandom.current().nextBytes(data);
      pong = ByteBufPayload.create(data);
    }
  }
}
