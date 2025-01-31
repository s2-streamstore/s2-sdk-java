package s2.config;

import java.util.Optional;

public final class Endpoints {
  public Address account;
  public BasinEndpoint basin;

  private Endpoints(Address account, BasinEndpoint basin) {
    this.account = account;
    this.basin = basin;
  }

  public static Endpoints forCell(Cloud cloud, String cell) {
    var endpoint = Address.fromString(String.format("%s.o.%s.s2.dev", cloud, cell));
    return new Endpoints(endpoint, new Direct(endpoint));
  }

  public static Endpoints fromEnvironment() {
    return manual(
        Optional.ofNullable(System.getenv("S2_CLOUD")),
        Optional.ofNullable(System.getenv("S2_ACCOUNT_ENDPOINT")),
        Optional.ofNullable(System.getenv("S2_BASIN_ENDPOINT")));
  }

  public static Endpoints manual(
      Optional<String> cloud, Optional<String> accountEndpoint, Optional<String> basinEndpoint) {
    var actualCloud = cloud.map(Cloud::fromString).orElse(Cloud.AWS);
    var endpoints = Endpoints.forCloud(actualCloud);
    accountEndpoint.ifPresent(endpoint -> endpoints.account = Address.fromString(endpoint));
    basinEndpoint.ifPresent(
        endpoint -> {
          if (endpoint.startsWith("{basin}.")) {
            endpoints.basin =
                new ParentZone(Address.fromString(endpoint.substring("{basin}.".length())));
          } else {
            endpoints.basin = new Direct(Address.fromString(endpoint));
          }
        });

    return endpoints;
  }

  public static Endpoints forCloud(Cloud cloud) {
    return new Endpoints(
        Address.fromString(String.format("%s.s2.dev", cloud)),
        new ParentZone(Address.fromString(String.format("b.%s.s2.dev", cloud))));
  }

  public boolean singleEndpoint() {
    if (this.basin instanceof Direct direct) {
      return this.account.equals(direct.address);
    } else if (this.basin instanceof ParentZone) {
      return false;
    } else {
      throw new IllegalStateException("Unexpected basin type: " + this.basin.getClass().getName());
    }
  }
}
