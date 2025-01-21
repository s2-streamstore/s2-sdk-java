package s2.types;

import java.time.Duration;

public abstract class RetentionPolicy {}

final class Age extends RetentionPolicy {
  final Duration age;

  Age(Duration age) {
    this.age = age;
  }
}
