package s2.types;

import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AppendRecord implements MeteredBytes, Serializable {

  public final List<Header> headers;
  public final ByteString body;

  private AppendRecord(List<Header> headers, ByteString body) {
    this.headers = headers;
    this.body = body;
  }

  public static AppendRecordBuilder newBuilder() {
    return new AppendRecordBuilder();
  }

  @Override
  public long meteredBytes() {
    return 8
        + (2L * this.headers.size())
        + this.headers.stream().map(h -> h.name().size() + h.value().size()).reduce(0, Integer::sum)
        + this.body.size();
  }

  public s2.v1alpha.AppendRecord toProto() {
    return s2.v1alpha.AppendRecord.newBuilder()
        .addAllHeaders(() -> this.headers.stream().map(Header::toProto).iterator())
        .setBody(this.body)
        .build();
  }

  public static class AppendRecordBuilder {
    private Optional<List<Header>> headers = Optional.empty();
    private Optional<ByteString> body = Optional.empty();

    public AppendRecordBuilder withHeaders(List<Header> headers) {
      this.headers = Optional.of(new ArrayList<>(headers));
      return this;
    }

    public AppendRecordBuilder withBody(byte[] body) {
      this.body = Optional.of(ByteString.copyFrom(body));
      return this;
    }

    public AppendRecordBuilder withBody(ByteString body) {
      this.body = Optional.of(body);
      return this;
    }

    public AppendRecord build() {
      List<Header> validatedHeaders = headers.orElse(new ArrayList<>());
      ByteString validatedBody = body.orElse(ByteString.EMPTY);

      var provisional = new AppendRecord(validatedHeaders, validatedBody);
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
