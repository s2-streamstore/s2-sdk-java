package s2.client;

import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import s2.types.ReadOutput;
import s2.types.ReadSessionRequest;

public class BufferedReadSession {
  private final StreamClient streamClient;
  private final ReadSession readSession;
  private final LinkedBlockingQueue<ReadOutput> queue;
  private final AtomicReference<Throwable> error = new AtomicReference<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  BufferedReadSession(StreamClient streamClient, ReadSessionRequest readSessionRequest) {
    this.streamClient = streamClient;
    this.queue = new LinkedBlockingQueue<>(10);
    this.readSession =
        streamClient.readSession(
            readSessionRequest,
            resp -> {
              try {
                System.out.println("putting another");
                queue.put(resp);
              } catch (InterruptedException e) {
                error.set(e);
              }
            },
            error::set);
    this.readSession
        .awaitCompletion()
        .addListener(
            () -> {
              this.closed.set(true);
            },
            this.streamClient.executor);
  }

  public boolean isClosed() {
    return queue.isEmpty() && closed.get();
  }

  public boolean hasNext() {
    return queue.peek() != null;
  }

  public Optional<ReadOutput> get() {
    return Optional.ofNullable(queue.poll());
  }
}
