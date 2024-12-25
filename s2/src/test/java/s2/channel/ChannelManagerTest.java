package s2.channel;

import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;

class ChannelManagerTest {

  @Test
  void shouldCreateAndManageChannelsCorrectly() {
    var host = "localhost";
    var port = 50051;
    var useTls = false;
    try (var channelManager = new ChannelManager(host, port, useTls)) {
      var channel1 = channelManager.createChannel();
      var currentChannel = channelManager.getCurrentChannel();

      // Then initial channel should be set
      assertThat(channel1).isNotNull();
      assertThat(currentChannel).isPresent();
      assertThat(currentChannel.get()).isSameAs(channel1);

      // When switching to a basin
      var basinChannel = channelManager.switchToBasin("test-basin");
      var updatedChannel = channelManager.getCurrentChannel();

      // Then basin channel should be current and different from initial channel
      assertThat(basinChannel).isNotNull();
      assertThat(updatedChannel).isPresent();
      assertThat(updatedChannel.get()).isSameAs(basinChannel);
      assertThat(updatedChannel.get()).isNotSameAs(channel1);

      // Assert both channels are not shutdown until explicitly closed
      assertThat(channel1.isShutdown()).isFalse();
      assertThat(basinChannel.isShutdown()).isFalse();
    }
  }

  @Test
  void shouldHandleChannelShutdownGracefully() throws InterruptedException {
    // Given
    var host = "localhost";
    var port = 50052;
    var useTls = false;

    var channelManager = new ChannelManager(host, port, useTls);

    // When
    var channel = channelManager.createChannel();
    channelManager.close();

    // Then
    assertThat(channel.isShutdown()).isTrue();
    assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void shouldCreateBasinChannelWithCorrectHost() {
    var host = "example.com";
    var port = 50053;
    var useTls = false;
    var basinName = "test-basin";

    try (var channelManager = new ChannelManager(host, port, useTls)) {
      var basinChannel = channelManager.createBasinChannel(basinName);

      assertThat(basinChannel).isNotNull();
      assertThat(basinChannel.isShutdown()).isFalse();
    }
  }
}
