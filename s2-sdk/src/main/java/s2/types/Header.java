package s2.types;

import com.google.protobuf.ByteString;

public class Header {
  public final ByteString name;
  public final ByteString value;

  public Header(ByteString name, ByteString value) {
    this.name = name;
    this.value = value;
  }

  public static Header fromProto(s2.v1alpha.Header protoHeader) {
    return new Header(protoHeader.getName(), protoHeader.getValue());
  }

  public s2.v1alpha.Header toProto() {
    return s2.v1alpha.Header.newBuilder().setName(name).setValue(value).build();
  }
}
