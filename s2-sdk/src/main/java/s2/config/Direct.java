package s2.config;

public final class Direct extends BasinEndpoint {
  public Direct(Address address) {
    super(address);
  }

  @Override
  public String toTarget(String basin) {
    return String.format("%s:%s", address.host, address.port);
  }
}
