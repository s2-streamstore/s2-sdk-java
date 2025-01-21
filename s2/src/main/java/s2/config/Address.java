package s2.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class Address {
  public final String host;
  public final int port;

  public Address(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public static Address fromString(String hostRepresentation) {
    try {
      URI uri = new URI(String.format("https://%s", hostRepresentation));
      var host = uri.getHost();
      var port = uri.getPort();
      if (host == null) {
        throw new IllegalArgumentException(
            "Unable to parse host from string " + hostRepresentation);
      }
      return new Address(host, port == -1 ? 443 : port);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("invalid URI format: " + hostRepresentation, e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Address that = (Address) obj;
    return (this.host.equals(that.host) && this.port == that.port);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }
}
