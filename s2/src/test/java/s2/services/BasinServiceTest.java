package s2.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import s2.v1alpha.BasinServiceGrpc;
import s2.v1alpha.S2.*;

@ExtendWith(MockitoExtension.class)
class BasinServiceTest {

  private BasinService basinService;
  private static final String TEST_STREAM = "test-stream";

  @Mock
  private ManagedChannel channel;

  @Mock
  private CallCredentials credentials;

  @Mock
  private BasinServiceGrpc.BasinServiceBlockingStub stub;

  private MockedStatic<BasinServiceGrpc> basinServiceGrpcMock;

  @BeforeEach
  void setUp() {
    basinServiceGrpcMock = mockStatic(BasinServiceGrpc.class);
    basinServiceGrpcMock.when(() -> BasinServiceGrpc.newBlockingStub(any(ManagedChannel.class)))
        .thenReturn(stub);
    when(stub.withCallCredentials(any(CallCredentials.class))).thenReturn(stub);
    
    basinService = new BasinService(channel, credentials);
  }

  @AfterEach
  void tearDown() {
    if (basinServiceGrpcMock != null) {
      basinServiceGrpcMock.close();
    }
  }

  @Test
  void listStreams_shouldReturnAllStreamsForEmptyPrefix() {
    var stream1 = StreamInfo.newBuilder().setName("stream1").build();
    var stream2 = StreamInfo.newBuilder().setName("stream2").build();
    
    var response = ListStreamsResponse.newBuilder()
        .addStreams(stream1)
        .addStreams(stream2)
        .setHasMore(false)
        .build();

    when(stub.listStreams(any(ListStreamsRequest.class))).thenReturn(response);

    var result = basinService.listStreams("");
    
    assertEquals(2, result.size());
    assertEquals("stream1", result.get(0).getName());
    assertEquals("stream2", result.get(1).getName());
    
    verify(stub).listStreams(argThat(request -> 
        request.getPrefix().isEmpty()
    ));
  }

  @Test
  void listStreams_shouldHandlePagination() {
    var stream1 = StreamInfo.newBuilder().setName("stream1").build();
    var stream2 = StreamInfo.newBuilder().setName("stream2").build();
    var stream3 = StreamInfo.newBuilder().setName("stream3").build();
    
    var response1 = ListStreamsResponse.newBuilder()
        .addStreams(stream1)
        .setHasMore(true)
        .build();
    
    var response2 = ListStreamsResponse.newBuilder()
        .addStreams(stream2)
        .addStreams(stream3)
        .setHasMore(false)
        .build();

    when(stub.listStreams(any(ListStreamsRequest.class)))
        .thenReturn(response1)
        .thenReturn(response2);

    var result = basinService.listStreams("test");
    
    assertEquals(3, result.size());
    assertEquals("stream1", result.get(0).getName());
    assertEquals("stream2", result.get(1).getName());
    assertEquals("stream3", result.get(2).getName());
    
    verify(stub, times(2)).listStreams(any(ListStreamsRequest.class));
  }

  @Test
  void createStream_shouldCreateStreamWithConfig() {
    var expectedStream = StreamInfo.newBuilder()
        .setName(TEST_STREAM)
        .build();
    
    var response = CreateStreamResponse.newBuilder()
        .setInfo(expectedStream)
        .build();

    var config = StreamConfig.newBuilder()
        .setStorageClass(StorageClass.STORAGE_CLASS_STANDARD)
        .build();

    when(stub.createStream(any(CreateStreamRequest.class))).thenReturn(response);

    var result = basinService.createStream(TEST_STREAM, config);

    assertEquals(TEST_STREAM, result.getName());
    verify(stub).createStream(argThat(request -> 
        request.getStream().equals(TEST_STREAM) &&
        request.getConfig().equals(config)
    ));
  }

  @Test
  void deleteStream_shouldDeleteExistingStream() {
    basinService.deleteStream(TEST_STREAM);

    verify(stub).deleteStream(argThat(request ->
        request.getStream().equals(TEST_STREAM)
    ));
  }

  @Test
  void createStream_shouldThrowWhenConfigIsNull() {
    assertThrows(NullPointerException.class, () ->
        basinService.createStream(TEST_STREAM, null)
    );
  }
}
