package s2.config;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Optional;

public class Config {
  public final String token;
  public final Endpoints endpoints;
  public final String userAgent;
  public final Integer maxRetries;
  public final Duration requestTimeout;
  public final AppendRetryPolicy appendRetryPolicy;

  private Config(
      String token,
      Endpoints endpoints,
      String userAgent,
      Integer maxRetries,
      Duration requestTimeout,
      AppendRetryPolicy appendRetryPolicy) {
    this.token = token;
    this.endpoints = endpoints;
    this.userAgent = userAgent;
    this.maxRetries = maxRetries;
    this.requestTimeout = requestTimeout;
    this.appendRetryPolicy = appendRetryPolicy;
  }

  public static ConfigBuilder newBuilder(String token) {
    return new ConfigBuilder(token);
  }

  public static final class ConfigBuilder {
    private final String token;
    private Optional<String> userAgent = Optional.empty();
    private Optional<Endpoints> endpoints = Optional.empty();
    private Optional<Duration> requestTimeout = Optional.empty();
    private Optional<Integer> maxRetries = Optional.empty();
    private Optional<AppendRetryPolicy> appendRetryPolicy = Optional.empty();

    ConfigBuilder(String token) {
      this.token = token;
    }

    public ConfigBuilder withUserAgent(String userAgent) {
      this.userAgent = Optional.of(userAgent);
      return this;
    }

    public ConfigBuilder withRequestTimeout(long timeout, TemporalUnit unit) {
      this.requestTimeout = Optional.of(Duration.of(timeout, unit));
      return this;
    }

    public ConfigBuilder withMaxRetries(int retries) {
      this.maxRetries = Optional.of(retries);
      return this;
    }

    public ConfigBuilder withEndpoints(Endpoints endpoints) {
      this.endpoints = Optional.of(endpoints);
      return this;
    }

    public ConfigBuilder withAppendRetryPolicy(AppendRetryPolicy appendRetryPolicy) {
      this.appendRetryPolicy = Optional.of(appendRetryPolicy);
      return this;
    }

    private void validate() {}

    public Config build() {
      validate();
      return new Config(
          this.token,
          this.endpoints.orElse(Endpoints.forCloud(Cloud.AWS)),
          this.userAgent.orElse("s2-sdk-java"),
          this.maxRetries.orElse(3),
          this.requestTimeout.orElse(Duration.ofSeconds(10)),
          this.appendRetryPolicy.orElse(AppendRetryPolicy.ALL));
    }
  }
}
