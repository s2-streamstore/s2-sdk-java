package s2.config;

public enum Cloud {
  AWS;

  public static Cloud fromString(String cloud) {
    return Cloud.valueOf(cloud.toUpperCase());
  }

  public String toString() {
    return this.name().toLowerCase();
  }
}
