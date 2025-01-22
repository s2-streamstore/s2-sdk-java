package s2.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

@ExtendWith(SystemStubsExtension.class)
class EndpointsTest {

  @Test
  void endpointAws() {
    var endpoints = Endpoints.forCloud(Cloud.AWS);
    assertThat(endpoints.account).isEqualTo(new Address("aws.s2.dev", 443));
    assertThat(endpoints.basin).isEqualTo(new ParentZone(new Address("b.aws.s2.dev", 443)));
  }

  @SystemStub private final EnvironmentVariables variables = new EnvironmentVariables();

  @Test
  void endpointFromEnvironment() {
    variables.set("S2_CLOUD", "bar");
    assertThrows(java.lang.IllegalArgumentException.class, Endpoints::fromEnvironment);

    variables.remove("S2_CLOUD");
    variables.set("S2_ACCOUNT_ENDPOINT", "my.custom.endpoint:4243");
    variables.set("S2_BASIN_ENDPOINT", "my.custom2.endpoint");
    var endpoints = Endpoints.fromEnvironment();
    assertThat(endpoints.account).isEqualTo(new Address("my.custom.endpoint", 4243));
    assertThat(endpoints.basin).isEqualTo(new Direct(new Address("my.custom2.endpoint", 443)));
  }
}
