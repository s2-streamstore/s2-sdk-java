package s2.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BearerTokenCallCredentialsTest {

  @Mock
  private CallCredentials.MetadataApplier metadataApplier;

  @Test
  void shouldAddBearerTokenToMetadata() {
    var token = "test-token";
    var credentials = new BearerTokenCallCredentials(token);
    Executor directExecutor = Runnable::run;

    credentials.applyRequestMetadata(null, directExecutor, metadataApplier);

    var metadataCaptor = ArgumentCaptor.forClass(Metadata.class);
    verify(metadataApplier).apply(metadataCaptor.capture());

    var capturedMetadata = metadataCaptor.getValue();
    var authHeader =
        capturedMetadata.get(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER));

    assertThat(authHeader).isNotNull().isEqualTo("Bearer " + token);
  }

  @Test
  void shouldRejectNullToken() {
    assertThatThrownBy(() -> new BearerTokenCallCredentials(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Token cannot be null or empty");
  }

  @Test
  void shouldRejectEmptyToken() {
    assertThatThrownBy(() -> new BearerTokenCallCredentials("  "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Token cannot be null or empty");
  }
}
