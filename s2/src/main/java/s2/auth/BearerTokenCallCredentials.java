package s2.auth;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.concurrent.Executor;

/**
 * Implementation of CallCredentials that adds a bearer token to request metadata. This class is
 * used to authenticate gRPC requests using a bearer token in the Authorization header with the
 * "Bearer" scheme.
 */
public class BearerTokenCallCredentials extends CallCredentials {
  /** Metadata key for the Authorization header. */
  private static final Metadata.Key<String> AUTHORIZATION_HEADER =
      Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);

  /** The bearer token value. */
  private final String token;

  /**
   * Creates new bearer token credentials.
   *
   * @param token The bearer token to use for authentication
   * @throws IllegalArgumentException if token is null or empty
   */
  public BearerTokenCallCredentials(String token) {
    if (token == null || token.trim().isEmpty()) {
      throw new IllegalArgumentException("Token cannot be null or empty");
    }
    this.token = token;
  }

  /**
   * Applies the bearer token to the request metadata. This method is called by the gRPC framework
   * before each request.
   *
   * @param requestInfo Information about the RPC being called
   * @param executor The executor to use for any asynchronous operations
   * @param metadataApplier Interface for applying the resulting metadata
   */
  @Override
  public void applyRequestMetadata(
      RequestInfo requestInfo, Executor executor, MetadataApplier metadataApplier) {
    executor.execute(
        () -> {
          try {
            Metadata headers = new Metadata();
            headers.put(AUTHORIZATION_HEADER, "Bearer " + token);
            metadataApplier.apply(headers);
          } catch (Throwable e) {
            metadataApplier.fail(Status.UNAUTHENTICATED.withCause(e));
          }
        });
  }

  /**
   * Returns a hash code value for this credentials implementation.
   *
   * @return A hash code value for this object
   */
  @Override
  public int hashCode() {
    return token.hashCode();
  }

  /**
   * Returns whether this credentials implementation is equal to another object. Two
   * BearerTokenCallCredentials are equal if they have the same token.
   *
   * @param other The object to compare with
   * @return true if the objects are equal, false otherwise
   */
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof BearerTokenCallCredentials)) {
      return false;
    }
    return token.equals(((BearerTokenCallCredentials) other).token);
  }
}
