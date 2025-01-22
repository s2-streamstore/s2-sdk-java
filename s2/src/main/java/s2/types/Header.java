package s2.types;

import com.google.protobuf.ByteString;

public record Header(ByteString name, ByteString value) {

  public static Header fromProto(s2.v1alpha.Header header) {
    return new Header(header.getName(), header.getValue());
  }

  public s2.v1alpha.Header toProto() {
    return s2.v1alpha.Header.newBuilder().setName(name).setValue(value).build();
  }
}
