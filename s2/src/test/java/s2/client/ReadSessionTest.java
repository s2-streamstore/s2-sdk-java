package s2.client;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import s2.config.Config;
import s2.types.Batch;
import s2.types.ReadLimit;
import s2.types.ReadOutput;
import s2.types.ReadSessionRequest;
import s2.v1alpha.StreamService.MockReadSessionStreamService;

public class ReadSessionTest {
  private Server server;
  private ManagedChannel channel;
  private StreamClient client;

  @BeforeEach
  public void setUp() throws Exception {
    String serverName = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new MockReadSessionStreamService())
            .build()
            .start();

    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

    Config config = Config.newBuilder("fake-token").withMaxRetries(3).build();
    client =
        new StreamClient(
            "test-stream",
            "test-basin",
            config,
            channel,
            Executors.newSingleThreadScheduledExecutor());
  }

  @AfterEach
  public void tearDown() throws Exception {
    channel.shutdownNow();
    server.shutdownNow();
  }

  @Test
  public void testReadSession() throws Exception {
    ReadSessionRequest request =
        ReadSessionRequest.newBuilder()
            .withStartSeqNum(0)
            .withReadLimit(ReadLimit.count(25))
            .build();

    ArrayList<ReadOutput> received = new ArrayList<>();

    var readSession =
        client.readSession(
            request,
            received::add,
            err -> {
              throw new RuntimeException(err.getMessage());
            });

    readSession.awaitCompletion().get();

    System.out.println(received);
    var flattenedRecords =
        received.stream()
            .flatMap(o -> ((Batch) o).sequencedRecordBatch().records().stream())
            .toList();
    assertThat(flattenedRecords.size()).isEqualTo(25);
    IntStream.range(0, flattenedRecords.size())
        .forEach(i -> assertThat(flattenedRecords.get(i).seqNum()).isEqualTo(i));
  }
}
