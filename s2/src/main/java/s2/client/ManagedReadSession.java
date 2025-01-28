package s2.client;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import s2.types.Batch;
import s2.types.ReadOutput;
import s2.types.ReadSessionRequest;

public class ManagedReadSession {

  private sealed interface ReadItem permits DataItem, ErrorItem, EndItem {}

  record DataItem(ReadOutput readOutput) implements ReadItem {}

  record ErrorItem(Throwable error) implements ReadItem {}

  record EndItem() implements ReadItem {}

  private final Semaphore bufferAvailable;
  private final LinkedBlockingQueue<ReadItem> queue;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ReadSession readSession;

  ManagedReadSession(
      StreamClient streamClient, ReadSessionRequest readSessionRequest, Integer maxBufferBytes) {
    this.queue = new LinkedBlockingQueue<>();
    this.bufferAvailable = new Semaphore(maxBufferBytes);
    this.readSession =
        streamClient.readSession(
            readSessionRequest,
            resp -> {
              try {
                if (resp instanceof Batch batch) {
                  bufferAvailable.acquire((int) batch.meteredBytes());
                }
                queue.put(new DataItem(resp));
              } catch (InterruptedException e) {
                queue.add(new ErrorItem(e));
              }
            },
            error -> {
              try {
                queue.put(new ErrorItem(error));
              } catch (InterruptedException e) {
                queue.add(new ErrorItem(e));
              }
            });
    this.readSession
        .awaitCompletion()
        .addListener(
            () -> {
              this.closed.set(true);
              this.queue.add(new EndItem());
            },
            streamClient.executor);
  }

  public boolean isClosed() {
    return !hasNext() && closed.get();
  }

  public boolean hasNext() {
    var peeked = queue.peek();
    if (peeked == null) {
      return false;
    } else if (peeked instanceof EndItem) {
      queue.poll();
      return false;
    }
    return true;
  }

  private Optional<ReadOutput> getInner(Optional<ReadItem> readItem) {
    var nextRead =
        readItem.flatMap(
            elem -> {
              if (elem instanceof ErrorItem item) {
                throw new RuntimeException(item.error);
              } else if (elem instanceof EndItem) {
                return Optional.empty();
              } else {
                return Optional.of(((DataItem) elem).readOutput);
              }
            });
    nextRead
        .map(nr -> (nr instanceof Batch batch) ? (int) batch.meteredBytes() : 0)
        .ifPresent(bufferAvailable::release);
    return nextRead;
  }

  public Optional<ReadOutput> get() {
    return getInner(Optional.ofNullable(queue.poll()));
  }

  public Optional<ReadOutput> get(Duration maxWait) throws InterruptedException {
    return getInner(Optional.ofNullable(queue.poll(maxWait.toMillis(), TimeUnit.MILLISECONDS)));
  }
}
