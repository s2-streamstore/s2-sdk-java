package s2.config;

public enum Cloud {
  AWS;

  public String toString() {
    return this.name().toLowerCase();
  }

  public static Cloud fromString(String cloud) {
    return Cloud.valueOf(cloud.toUpperCase());
  }
}
