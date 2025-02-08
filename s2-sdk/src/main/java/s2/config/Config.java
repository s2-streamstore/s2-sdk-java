package s2.config;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Optional;

public class Config {
  public final String token;
  public final AppendRetryPolicy appendRetryPolicy;
  public final Endpoints endpoints;
  public final Integer maxAppendInflightBytes;
  public final Integer maxRetries;
  public final Duration requestTimeout;
  public final Duration retryDelay;
  public final String userAgent;
  public final Boolean compression;

  private Config(
      String token,
      AppendRetryPolicy appendRetryPolicy,
      Endpoints endpoints,
      Integer maxAppendInflightBytes,
      Integer maxRetries,
      Duration requestTimeout,
      Duration retryDelay,
      String userAgent,
      Boolean compression) {
    this.token = token;
    this.appendRetryPolicy = appendRetryPolicy;
    this.endpoints = endpoints;
    this.maxAppendInflightBytes = maxAppendInflightBytes;
    this.maxRetries = maxRetries;
    this.requestTimeout = requestTimeout;
    this.retryDelay = retryDelay;
    this.userAgent = userAgent;
    this.compression = compression;
  }

  public static ConfigBuilder newBuilder(String token) {
    return new ConfigBuilder(token);
  }

  public static final class ConfigBuilder {
    private final String token;
    private Optional<AppendRetryPolicy> appendRetryPolicy = Optional.empty();
    private Optional<Endpoints> endpoints = Optional.empty();
    private Optional<Integer> maxAppendInflightBytes = Optional.empty();
    private Optional<Integer> maxRetries = Optional.empty();
    private Optional<Duration> requestTimeout = Optional.empty();
    private Optional<Duration> retryDelay = Optional.empty();
    private Optional<String> userAgent = Optional.empty();
    private Optional<Boolean> compression = Optional.empty();

    ConfigBuilder(String token) {
      this.token = token;
    }

    public ConfigBuilder withAppendRetryPolicy(AppendRetryPolicy appendRetryPolicy) {
      this.appendRetryPolicy = Optional.of(appendRetryPolicy);
      return this;
    }

    public ConfigBuilder withEndpoints(Endpoints endpoints) {
      this.endpoints = Optional.of(endpoints);
      return this;
    }

    public ConfigBuilder withMaxAppendInflightBytes(int maxAppendInflightBytes) {
      this.maxAppendInflightBytes = Optional.of(maxAppendInflightBytes);
      return this;
    }

    public ConfigBuilder withMaxRetries(int retries) {
      this.maxRetries = Optional.of(retries);
      return this;
    }

    public ConfigBuilder withRequestTimeout(long timeout, TemporalUnit unit) {
      this.requestTimeout = Optional.of(Duration.of(timeout, unit));
      return this;
    }

    public ConfigBuilder withRetryDelay(Duration delay) {
      this.retryDelay = Optional.of(delay);
      return this;
    }

    public ConfigBuilder withUserAgent(String userAgent) {
      this.userAgent = Optional.of(userAgent);
      return this;
    }

    public ConfigBuilder withCompression(Boolean compression) {
      this.compression = Optional.of(compression);
      return this;
    }

    public Config build() {
      validate();
      return new Config(
          this.token,
          this.appendRetryPolicy.orElse(AppendRetryPolicy.ALL),
          this.endpoints.orElse(Endpoints.forCloud(Cloud.AWS)),
          this.maxAppendInflightBytes.orElse(Integer.MAX_VALUE),
          this.maxRetries.orElse(3),
          this.requestTimeout.orElse(Duration.ofSeconds(10)),
          this.retryDelay.orElse(Duration.ofMillis(50)),
          this.userAgent.orElse("s2-sdk-java"),
          this.compression.orElse(false));
    }

    private void validate() {
      this.maxRetries.ifPresent(
          maxRetries -> {
            if (maxRetries < 0) {
              throw new IllegalArgumentException("maxRetries must be a positive integer");
            }
          });

      this.requestTimeout.ifPresent(
          requestTimeout -> {
            if (requestTimeout.isNegative()) {
              throw new IllegalArgumentException("requestTimeout must be a positive duration");
            }
          });

      this.maxAppendInflightBytes.ifPresent(
          bytes -> {
            if (bytes < 0) {
              throw new IllegalArgumentException("bytes must be a positive integer");
            }
          });
    }
  }
}
