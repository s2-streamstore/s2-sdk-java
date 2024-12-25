package s2.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.grpc.ManagedChannel;
import io.grpc.CallCredentials;
import s2.channel.ChannelManager;

@ExtendWith(MockitoExtension.class)
public class ClientTest {
  @Mock
  private ManagedChannel mockChannel;

  @Mock
  private CallCredentials mockCredentials;

  @Mock
  private ChannelManager mockChannelManager;

  private Client client;
  private static final String TEST_BASIN = "test-basin";

  @BeforeEach
  void setUp() {
    client = new Client(mockChannel, mockCredentials, mockChannelManager);
  }

  @Test
  void shouldInitializeAllServicesOnConstruction() {
    assertNotNull(client.account(), "Account service should be initialized");
    assertNotNull(client.basin(), "Basin service should be initialized");
    assertNotNull(client.stream(), "Stream service should be initialized");
    assertNotNull(client.streamAsync(), "Async stream service should be initialized");
  }

  @Test
  void shouldUpdateServicesWhenSwitchingBasins() {
    var newChannel = mock(ManagedChannel.class);
    when(mockChannelManager.switchToBasin(TEST_BASIN)).thenReturn(newChannel);

    client.useBasin(TEST_BASIN);

    verify(mockChannelManager).switchToBasin(TEST_BASIN);
  }

  @Test
  void shouldThrowWhenSwitchingBasinsWithInjectedChannel() {
    // Given
    var clientWithInjectedChannel = new Client(mockChannel, mockCredentials, null);

    // When/Then
    UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
        () -> clientWithInjectedChannel.useBasin(TEST_BASIN),
        "Should throw when switching basins with injected channel");

    clientWithInjectedChannel.close();
    assertTrue(exception.getMessage().contains("Basin switching is not supported"));
  }

  @Test
  void shouldCloseChannelManagerWhenClosing() {
    client.close();

    verify(mockChannelManager).close();
  }

  @Test
  void shouldNotCloseChannelManagerWhenUsingInjectedChannel() {
    var clientWithInjectedChannel = new Client(mockChannel, mockCredentials, null);

    clientWithInjectedChannel.close();

    verifyNoInteractions(mockChannelManager);
  }

  @Test
  void shouldWorkWithTryWithResources() {
    try (Client autoCloseableClient = client) {
      assertNotNull(autoCloseableClient.account());
      assertNotNull(autoCloseableClient.basin());
    }

    verify(mockChannelManager).close();
  }

  @Test
  void shouldMaintainServiceStateAfterBasinSwitch() {

    var accountBefore = client.account();
    var basinBefore = client.basin();
    var streamBefore = client.stream();
    var asyncStreamBefore = client.streamAsync();

    var newChannel = mock(ManagedChannel.class);
    when(mockChannelManager.switchToBasin(TEST_BASIN)).thenReturn(newChannel);
    client.useBasin(TEST_BASIN);

    assertSame(accountBefore, client.account(), "Account service instance should be maintained");
    assertSame(basinBefore, client.basin(), "Basin service instance should be maintained");
    assertSame(streamBefore, client.stream(), "Stream service instance should be maintained");
    assertSame(asyncStreamBefore, client.streamAsync(),
        "Async stream service instance should be maintained");
  }

  @Test
  void shouldHandleMultipleBasinSwitches() {
    var basin1 = "basin1";
    var basin2 = "basin2";
    var channel1 = mock(ManagedChannel.class);
    var channel2 = mock(ManagedChannel.class);

    when(mockChannelManager.switchToBasin(basin1)).thenReturn(channel1);
    when(mockChannelManager.switchToBasin(basin2)).thenReturn(channel2);

    client.useBasin(basin1);
    client.useBasin(basin2);

    verify(mockChannelManager).switchToBasin(basin1);
    verify(mockChannelManager).switchToBasin(basin2);
  }
}
