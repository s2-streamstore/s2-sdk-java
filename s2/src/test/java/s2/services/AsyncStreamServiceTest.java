package s2.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import s2.v1alpha.S2.*;
import s2.v1alpha.StreamServiceGrpc;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@ExtendWith(MockitoExtension.class)
class AsyncStreamServiceTest {

  private AsyncStreamService asyncStreamService;
  private static final String TEST_STREAM = "test-stream";

  @Mock
  private ManagedChannel channel;

  @Mock
  private CallCredentials credentials;

  @Mock
  private StreamServiceGrpc.StreamServiceFutureStub futureStub;

  @Mock
  private StreamServiceGrpc.StreamServiceStub asyncStub;

  @Captor
  private ArgumentCaptor<StreamObserver<AppendResponse>> appendResponseObserverCaptor;

  @Captor
  private ArgumentCaptor<StreamObserver<ReadResponse>> readResponseObserverCaptor;

  @Captor
  private ArgumentCaptor<StreamObserver<CheckTailResponse>> checkTailResponseObserverCaptor;

  @Captor
  private ArgumentCaptor<StreamObserver<AppendSessionResponse>> appendSessionResponseObserverCaptor;

  @Captor
  private ArgumentCaptor<StreamObserver<ReadSessionResponse>> readSessionResponseObserverCaptor;

  private MockedStatic<StreamServiceGrpc> streamServiceGrpcMock;

  @BeforeEach
  void setUp() {
    streamServiceGrpcMock = mockStatic(StreamServiceGrpc.class);
    streamServiceGrpcMock.when(() -> StreamServiceGrpc.newFutureStub(any(ManagedChannel.class)))
        .thenReturn(futureStub);
    streamServiceGrpcMock.when(() -> StreamServiceGrpc.newStub(any(ManagedChannel.class)))
        .thenReturn(asyncStub);

    when(futureStub.withCallCredentials(any(CallCredentials.class))).thenReturn(futureStub);
    when(asyncStub.withCallCredentials(any(CallCredentials.class))).thenReturn(asyncStub);

    asyncStreamService = new AsyncStreamService(channel, credentials);
  }

  @AfterEach
  void tearDown() {
    if (streamServiceGrpcMock != null) {
      streamServiceGrpcMock.close();
    }
  }

  @Test
  void appendAsync_shouldCompleteSuccessfully() throws Exception {
    var record = AppendRecord.newBuilder()
        .setBody(com.google.protobuf.ByteString.copyFromUtf8("data")).build();

    var expectedOutput =
        AppendOutput.newBuilder().setStartSeqNum(1).setEndSeqNum(1).setNextSeqNum(2).build();

    doAnswer(invocation -> {
      var observer = invocation.<StreamObserver<AppendResponse>>getArgument(1);
      observer.onNext(AppendResponse.newBuilder().setOutput(expectedOutput).build());
      observer.onCompleted();
      return null;
    }).when(asyncStub).append(any(AppendRequest.class), any(StreamObserver.class));

    var future = asyncStreamService.appendAsync(TEST_STREAM, Collections.singletonList(record));
    var result = future.get(5, TimeUnit.SECONDS);

    assertEquals(1, result.getStartSeqNum());
    assertEquals(1, result.getEndSeqNum());
    assertEquals(2, result.getNextSeqNum());

    verify(asyncStub).append(argThat(request -> request.getInput().getStream().equals(TEST_STREAM)
        && request.getInput().getRecordsCount() == 1), any(StreamObserver.class));
  }

  @Test
  void appendAsync_shouldHandleError() {
    var record = AppendRecord.newBuilder()
        .setBody(com.google.protobuf.ByteString.copyFromUtf8("data")).build();

    var expectedException = new RuntimeException("Test error");

    doAnswer(invocation -> {
      var observer = invocation.<StreamObserver<AppendResponse>>getArgument(1);
      observer.onError(expectedException);
      return null;
    }).when(asyncStub).append(any(AppendRequest.class), any(StreamObserver.class));

    var future = asyncStreamService.appendAsync(TEST_STREAM, Collections.singletonList(record));

    var exception = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertEquals(expectedException, exception.getCause());
  }

  @Test
  void readAsync_shouldCompleteSuccessfully() throws Exception {
    var record = SequencedRecord.newBuilder().setSeqNum(1)
        .setBody(com.google.protobuf.ByteString.copyFromUtf8("data")).build();

    var batch = SequencedRecordBatch.newBuilder().addRecords(record).build();

    var readOutput = ReadOutput.newBuilder().setBatch(batch).build();

    doAnswer(invocation -> {
      var observer = invocation.<StreamObserver<ReadResponse>>getArgument(1);
      observer.onNext(ReadResponse.newBuilder().setOutput(readOutput).build());
      observer.onCompleted();
      return null;
    }).when(asyncStub).read(any(ReadRequest.class), any(StreamObserver.class));

    var limit = ReadLimit.newBuilder().setCount(10).setBytes(1024).build();

    var future = asyncStreamService.readAsync(TEST_STREAM, 1, limit);
    var result = future.get(5, TimeUnit.SECONDS);

    assertTrue(result.hasBatch());
    assertEquals(1, result.getBatch().getRecordsCount());
    assertEquals(1, result.getBatch().getRecords(0).getSeqNum());

    verify(asyncStub)
        .read(
            argThat(request -> request.getStream().equals(TEST_STREAM)
                && request.getStartSeqNum() == 1 && request.getLimit().equals(limit)),
            any(StreamObserver.class));
  }

  @Test
  void checkTailAsync_shouldCompleteSuccessfully() throws Exception {
    doAnswer(invocation -> {
      var observer = invocation.<StreamObserver<CheckTailResponse>>getArgument(1);
      observer.onNext(CheckTailResponse.newBuilder().setNextSeqNum(100).build());
      observer.onCompleted();
      return null;
    }).when(asyncStub).checkTail(any(CheckTailRequest.class), any(StreamObserver.class));

    var future = asyncStreamService.checkTailAsync(TEST_STREAM);
    var result = future.get(5, TimeUnit.SECONDS);

    assertEquals(100, result);
    verify(asyncStub).checkTail(argThat(request -> request.getStream().equals(TEST_STREAM)),
        any(StreamObserver.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  void openAppendSession_shouldHandleStreamingAppends() {
    @SuppressWarnings("unchecked")
    var requestObserver = mock(StreamObserver.class);
    when(asyncStub.appendSession(any())).thenReturn(requestObserver);

    var onResponse = mock(Consumer.class);
    var onError = mock(Consumer.class);

    asyncStreamService.openAppendSession(TEST_STREAM, onResponse, onError);

    verify(asyncStub).appendSession(appendSessionResponseObserverCaptor.capture());
    var capturedObserver = appendSessionResponseObserverCaptor.getValue();

    var output =
        AppendOutput.newBuilder().setStartSeqNum(1).setEndSeqNum(1).setNextSeqNum(2).build();

    capturedObserver.onNext(AppendSessionResponse.newBuilder().setOutput(output).build());
    verify(onResponse).accept(
        argThat(response -> ((AppendSessionResponse) response).getOutput().getStartSeqNum() == 1
            && ((AppendSessionResponse) response).getOutput().getEndSeqNum() == 1
            && ((AppendSessionResponse) response).getOutput().getNextSeqNum() == 2));

    var error = new RuntimeException("Test error");
    capturedObserver.onError(error);
    verify(onError).accept(error);
  }

  @SuppressWarnings("unchecked")
  @Test
  void openReadSession_shouldHandleStreamingReads() {
    var onResponse = mock(Consumer.class);
    var onError = mock(Consumer.class);

    var limit = ReadLimit.newBuilder().setCount(10).setBytes(1024).build();

    asyncStreamService.openReadSession(TEST_STREAM, 1, limit, onResponse, onError);

    verify(asyncStub).readSession(any(ReadSessionRequest.class),
        readSessionResponseObserverCaptor.capture());
    var capturedObserver = readSessionResponseObserverCaptor.getValue();

    var record = SequencedRecord.newBuilder().setSeqNum(1)
        .setBody(com.google.protobuf.ByteString.copyFromUtf8("data")).build();

    var batch = SequencedRecordBatch.newBuilder().addRecords(record).build();

    var readOutput = ReadOutput.newBuilder().setBatch(batch).build();

    capturedObserver.onNext(ReadSessionResponse.newBuilder().setOutput(readOutput).build());
    verify(onResponse).accept(argThat(response -> ((ReadSessionResponse) response).getOutput()
        .getBatch().getRecordsCount() == 1
        && ((ReadSessionResponse) response).getOutput().getBatch().getRecords(0).getSeqNum() == 1));

    var error = new RuntimeException("Test error");
    capturedObserver.onError(error);
    verify(onError).accept(error);
  }

  @Test
  void appendAsync_shouldThrowWhenRecordsIsNull() {
    assertThrows(NullPointerException.class,
        () -> asyncStreamService.appendAsync(TEST_STREAM, null));
  }

  @Test
  void openAppendSession_shouldThrowWhenCallbacksAreNull() {
    assertAll(
        () -> assertThrows(NullPointerException.class,
            () -> asyncStreamService.openAppendSession(TEST_STREAM, null, mock(Consumer.class))),
        () -> assertThrows(NullPointerException.class,
            () -> asyncStreamService.openAppendSession(TEST_STREAM, mock(Consumer.class), null)));
  }

  @Test
  void openReadSession_shouldThrowWhenCallbacksAreNull() {
    assertAll(
        () -> assertThrows(NullPointerException.class,
            () -> asyncStreamService.openReadSession(TEST_STREAM, 1, null, null,
                mock(Consumer.class))),
        () -> assertThrows(NullPointerException.class, () -> asyncStreamService
            .openReadSession(TEST_STREAM, 1, null, mock(Consumer.class), null)));
  }
}
