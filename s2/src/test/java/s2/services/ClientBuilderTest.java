package s2.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.grpc.ManagedChannel;
import io.grpc.CallCredentials;
import s2.auth.BearerTokenCallCredentials;

@ExtendWith(MockitoExtension.class)
public class ClientBuilderTest {
  @Mock
  private ManagedChannel mockChannel;

  private static final String TEST_HOST = "test.s2.dev";
  private static final String TEST_TOKEN = "test-token";
  private static final String TEST_BASIN = "test-basin";

  @Test
  void builderShouldCreateClientWithMinimalParameters() {
    Client client = Client.newBuilder().host(TEST_HOST).bearerToken(TEST_TOKEN).build();

    assertNotNull(client);
    assertNotNull(client.account());
    assertNotNull(client.basin());
    assertNotNull(client.stream());
    assertNotNull(client.streamAsync());
  }

  @Test
  void builderShouldCreateClientWithInjectedChannel() {
    CallCredentials credentials = new BearerTokenCallCredentials(TEST_TOKEN);
    Client client = Client.newBuilder().channel(mockChannel).credentials(credentials).build();

    assertNotNull(client);
  }

  @Test
  void builderShouldThrowWhenMissingRequiredParameters() {
    ClientBuilder builder = Client.newBuilder();

    assertThrows(IllegalStateException.class, builder::build,
        "Should throw when no parameters are set");

    assertThrows(IllegalStateException.class, () -> builder.host(TEST_HOST).build(),
        "Should throw when bearer token is missing");

    ClientBuilder tokenBuilder = Client.newBuilder();
    assertThrows(IllegalStateException.class, () -> tokenBuilder.bearerToken(TEST_TOKEN).build(),
        "Should throw when host is missing");
  }

  @Test
  void useBasinShouldThrowWithInjectedChannel() {
    CallCredentials credentials = new BearerTokenCallCredentials(TEST_TOKEN);
    Client client = Client.newBuilder().channel(mockChannel).credentials(credentials).build();

    assertThrows(UnsupportedOperationException.class, () -> client.useBasin(TEST_BASIN),
        "Should throw when using injected channel");
  }

  @Test
  void useBasinShouldWorkWithManagedChannel() {
    Client client = Client.newBuilder().host(TEST_HOST).bearerToken(TEST_TOKEN).build();

    // This should not throw
    assertDoesNotThrow(() -> client.useBasin(TEST_BASIN));
  }

  @Test
  void closeShouldWorkWithManagedChannel() {
    Client client = Client.newBuilder().host(TEST_HOST).bearerToken(TEST_TOKEN).build();

    // This should not throw
    assertDoesNotThrow(client::close);
  }

  @Test
  void closeShouldNotAffectInjectedChannel() {
    CallCredentials credentials = new BearerTokenCallCredentials(TEST_TOKEN);
    Client client = Client.newBuilder().channel(mockChannel).credentials(credentials).build();

    client.close();

    // Verify channel was not shut down
    verify(mockChannel, never()).shutdown();
  }
}
