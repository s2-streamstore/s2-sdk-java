package s2.config;

public final class ParentZone extends BasinEndpoint {
  public ParentZone(Address address) {
    super(address);
  }

  @Override
  public String toTarget(String basin) {
    return String.format("%s.%s:%s", basin, address.host, address.port);
  }
}
