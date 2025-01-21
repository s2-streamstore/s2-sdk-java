package s2.types;

import com.google.protobuf.FieldMask;

// TODO
public class ReconfigureBasinRequest {

  public s2.v1alpha.ReconfigureBasinRequest toProto() {
    var builder = s2.v1alpha.ReconfigureBasinRequest.newBuilder();
    return builder.setMask(FieldMask.newBuilder().addPaths("").build()).build();
  }
}
