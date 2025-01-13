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
import s2.v1alpha.*;

import java.util.Arrays;
import java.util.Collections;

@ExtendWith(MockitoExtension.class)
class StreamServiceTest {

  private StreamService streamService;
  private static final String TEST_STREAM = "test-stream";

  @Mock
  private ManagedChannel channel;

  @Mock
  private CallCredentials credentials;

  @Mock
  private StreamServiceGrpc.StreamServiceBlockingStub stub;

  private MockedStatic<StreamServiceGrpc> streamServiceGrpcMock;

  @BeforeEach
  void setUp() {
    streamServiceGrpcMock = mockStatic(StreamServiceGrpc.class);
    streamServiceGrpcMock.when(() -> StreamServiceGrpc.newBlockingStub(any(ManagedChannel.class)))
        .thenReturn(stub);
    when(stub.withCallCredentials(any(CallCredentials.class))).thenReturn(stub);
    
    streamService = new StreamService(channel, credentials);
  }

  @AfterEach
  void tearDown() {
    if (streamServiceGrpcMock != null) {
      streamServiceGrpcMock.close();
    }
  }

  @Test
  void append_shouldAppendRecordsToStream() {
    var record1 = AppendRecord.newBuilder()
        .setBody(com.google.protobuf.ByteString.copyFromUtf8("data1"))
        .build();
    var record2 = AppendRecord.newBuilder()
        .setBody(com.google.protobuf.ByteString.copyFromUtf8("data2"))
        .build();

    var expectedOutput = AppendOutput.newBuilder()
        .setStartSeqNum(1)
        .setEndSeqNum(2)
        .setNextSeqNum(3)
        .build();

    var response = AppendResponse.newBuilder()
        .setOutput(expectedOutput)
        .build();

    when(stub.append(any(AppendRequest.class))).thenReturn(response);

    var records = Arrays.asList(record1, record2);
    var result = streamService.append(TEST_STREAM, records);

    assertEquals(1, result.getStartSeqNum());
    assertEquals(2, result.getEndSeqNum());
    assertEquals(3, result.getNextSeqNum());

    verify(stub).append(argThat(request -> 
        request.getInput().getStream().equals(TEST_STREAM) &&
        request.getInput().getRecordsList().equals(records)
    ));
  }

  @Test
  void read_shouldReadRecordsFromStream() {
    var record = SequencedRecord.newBuilder()
        .setSeqNum(1)
        .setBody(com.google.protobuf.ByteString.copyFromUtf8("data"))
        .build();

    var batch = SequencedRecordBatch.newBuilder()
        .addRecords(record)
        .build();

    var readOutput = ReadOutput.newBuilder()
        .setBatch(batch)
        .build();

    var response = ReadResponse.newBuilder()
        .setOutput(readOutput)
        .build();

    var limit = ReadLimit.newBuilder()
        .setCount(10)
        .setBytes(1024)
        .build();

    when(stub.read(any(ReadRequest.class))).thenReturn(response);

    var result = streamService.read(TEST_STREAM, 1, limit);

    assertTrue(result.hasBatch());
    assertEquals(1, result.getBatch().getRecordsCount());
    assertEquals(1, result.getBatch().getRecords(0).getSeqNum());

    verify(stub).read(argThat(request -> 
        request.getStream().equals(TEST_STREAM) &&
        request.getStartSeqNum() == 1 &&
        request.getLimit().equals(limit)
    ));
  }

  @Test
  void read_shouldWorkWithoutLimit() {
    var readOutput = ReadOutput.newBuilder()
        .setNextSeqNum(1)
        .build();

    var response = ReadResponse.newBuilder()
        .setOutput(readOutput)
        .build();

    when(stub.read(any(ReadRequest.class))).thenReturn(response);

    var result = streamService.read(TEST_STREAM, 1, null);

    assertTrue(result.hasNextSeqNum());
    assertEquals(1, result.getNextSeqNum());

    verify(stub).read(argThat(request -> 
        request.getStream().equals(TEST_STREAM) &&
        request.getStartSeqNum() == 1 &&
        !request.hasLimit()
    ));
  }

  @Test
  void checkTail_shouldReturnNextSequenceNumber() {
    var response = CheckTailResponse.newBuilder()
        .setNextSeqNum(100)
        .build();

    when(stub.checkTail(any(CheckTailRequest.class))).thenReturn(response);

    var result = streamService.checkTail(TEST_STREAM);

    assertEquals(100, result);
    verify(stub).checkTail(argThat(request ->
        request.getStream().equals(TEST_STREAM)
    ));
  }

  @Test
  void append_shouldThrowWhenRecordsIsNull() {
    assertThrows(NullPointerException.class, () ->
        streamService.append(TEST_STREAM, null)
    );
  }

  @Test
  void append_shouldWorkWithEmptyRecords() {
    var expectedOutput = AppendOutput.newBuilder()
        .setStartSeqNum(1)
        .setEndSeqNum(1)
        .setNextSeqNum(2)
        .build();

    var response = AppendResponse.newBuilder()
        .setOutput(expectedOutput)
        .build();

    when(stub.append(any(AppendRequest.class))).thenReturn(response);

    var result = streamService.append(TEST_STREAM, Collections.emptyList());

    assertEquals(1, result.getStartSeqNum());
    assertEquals(1, result.getEndSeqNum());
    assertEquals(2, result.getNextSeqNum());
  }
}
