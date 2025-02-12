package s2.channel;

import io.grpc.ManagedChannelBuilder;
import s2.config.Config;

public class ManagedChannelFactory {
  public static AutoClosableManagedChannel forAccountService(Config config) {
    return AutoClosableManagedChannel.of(
        ManagedChannelBuilder.forAddress(
                config.endpoints.account.host, config.endpoints.account.port)
            .build());
  }

  public static AutoClosableManagedChannel forBasinOrStreamService(
      Config config, String basinName) {
    return AutoClosableManagedChannel.of(
        ManagedChannelBuilder.forTarget(config.endpoints.basin.toTarget(basinName)).build());
  }
}
