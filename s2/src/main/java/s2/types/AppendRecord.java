package s2.types;

import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AppendRecord implements MeteredBytes, Serializable {

  private final List<Header> headers;
  private final byte[] bytes;

  // Private constructor to prevent direct instantiation
  private AppendRecord(List<Header> headers, byte[] bytes) {
    this.headers = headers;
    this.bytes = bytes;
  }

  // Factory method to create a builder
  public static AppendRecordBuilder newBuilder() {
    return new AppendRecordBuilder();
  }

  public List<Header> getHeaders() {
    return headers;
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public long meteredBytes() {
    return 8
        + (2L * this.headers.size())
        + this.headers.stream().map(h -> h.name().size() + h.value().size()).reduce(0, Integer::sum)
        + this.bytes.length;
  }

  public s2.v1alpha.AppendRecord toProto() {
    return s2.v1alpha.AppendRecord.newBuilder()
        .addAllHeaders(() -> this.headers.stream().map(Header::toProto).iterator())
        .setBody(ByteString.copyFrom(this.bytes))
        .build();
  }

  // Builder class for constructing AppendRecord
  public static class AppendRecordBuilder {
    private Optional<List<Header>> headers = Optional.empty();
    private Optional<byte[]> bytes = Optional.empty();

    // Set the headers with validation (if needed)
    public AppendRecordBuilder withHeaders(List<Header> headers) {
      this.headers = Optional.of(new ArrayList<>(headers));
      return this;
    }

    // Set the bytes with validation (if needed)
    public AppendRecordBuilder withBytes(byte[] bytes) {
      this.bytes = Optional.of(bytes);
      return this;
    }

    // Build the AppendRecord with optional validation before returning
    public AppendRecord build() {
      List<Header> validatedHeaders = headers.orElse(new ArrayList<>());
      byte[] validatedBytes = bytes.orElse(new byte[0]);

      // Example validation: check that the headers are not empty or that bytes are not empty
      var provisional = new AppendRecord(validatedHeaders, validatedBytes);
      var meteredBytes = provisional.meteredBytes();
      if (meteredBytes > 1024 * 1024) {
        throw new IllegalStateException(
            String.format(
                "AppendRecord would exceed the maximum allowed metered size of 1MiB, currently %s bytes.",
                meteredBytes));
      }
      return provisional;
    }
  }
}
