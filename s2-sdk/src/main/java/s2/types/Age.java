package s2.types;

import java.time.Duration;

public final class Age extends RetentionPolicy {
  public final Duration age;

  public Age(Duration age) {
    this.age = age;
  }
}
