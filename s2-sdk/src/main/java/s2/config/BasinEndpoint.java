package s2.config;

import java.util.Objects;

public abstract class BasinEndpoint {
  public final Address address;

  BasinEndpoint(Address address) {
    this.address = address;
  }

  public abstract String toTarget(String basin);

  @Override
  public int hashCode() {
    return Objects.hash(address);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    BasinEndpoint that = (BasinEndpoint) obj;
    return Objects.equals(address, that.address);
  }
}
