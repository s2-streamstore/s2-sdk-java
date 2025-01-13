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
import s2.v1alpha.AccountServiceGrpc;
import s2.v1alpha.S2.*;

@ExtendWith(MockitoExtension.class)
class AccountServiceTest {

  private AccountService accountService;
  private static final String TEST_BASIN = "test-basin";

  @Mock
  private ManagedChannel channel;

  @Mock
  private CallCredentials credentials;

  @Mock
  private AccountServiceGrpc.AccountServiceBlockingStub stub;

  private MockedStatic<AccountServiceGrpc> accountServiceGrpcMock;

  @BeforeEach
  void setUp() {
    accountServiceGrpcMock = mockStatic(AccountServiceGrpc.class);
    accountServiceGrpcMock.when(() -> AccountServiceGrpc.newBlockingStub(any(ManagedChannel.class)))
        .thenReturn(stub);
    when(stub.withCallCredentials(any(CallCredentials.class))).thenReturn(stub);
    
    accountService = new AccountService(channel, credentials);
  }

  @AfterEach
  void tearDown() {
    if (accountServiceGrpcMock != null) {
      accountServiceGrpcMock.close();
    }
  }

  @Test
  void listBasins_shouldReturnAllBasinsForEmptyPrefix() {
    var basin1 = BasinInfo.newBuilder().setName("basin1").build();
    var basin2 = BasinInfo.newBuilder().setName("basin2").build();
    
    var response1 = ListBasinsResponse.newBuilder()
        .addBasins(basin1)
        .addBasins(basin2)
        .setHasMore(false)
        .build();

    when(stub.listBasins(any(ListBasinsRequest.class))).thenReturn(response1);

    var result = accountService.listBasins("");
    
    assertEquals(2, result.size());
    assertEquals("basin1", result.get(0).getName());
    assertEquals("basin2", result.get(1).getName());
    
    verify(stub).listBasins(argThat(request -> 
        request.getPrefix().isEmpty()
    ));
  }

  @Test
  void listBasins_shouldHandlePagination() {
    var basin1 = BasinInfo.newBuilder().setName("basin1").build();
    var basin2 = BasinInfo.newBuilder().setName("basin2").build();
    var basin3 = BasinInfo.newBuilder().setName("basin3").build();
    
    var response1 = ListBasinsResponse.newBuilder()
        .addBasins(basin1)
        .setHasMore(true)
        .build();
    
    var response2 = ListBasinsResponse.newBuilder()
        .addBasins(basin2)
        .addBasins(basin3)
        .setHasMore(false)
        .build();

    when(stub.listBasins(any(ListBasinsRequest.class)))
        .thenReturn(response1)
        .thenReturn(response2);

    var result = accountService.listBasins("test");
    
    assertEquals(3, result.size());
    assertEquals("basin1", result.get(0).getName());
    assertEquals("basin2", result.get(1).getName());
    assertEquals("basin3", result.get(2).getName());
    
    verify(stub, times(2)).listBasins(any(ListBasinsRequest.class));
  }

  @Test
  void createBasin_shouldCreateBasinWithConfig() {
    var expectedBasin = BasinInfo.newBuilder()
        .setName(TEST_BASIN)
        .build();
    
    var response = CreateBasinResponse.newBuilder()
        .setInfo(expectedBasin)
        .build();

    var streamConfig = StreamConfig.newBuilder()
        .setStorageClass(StorageClass.STORAGE_CLASS_STANDARD)
        .build();

    var config = BasinConfig.newBuilder()
        .setDefaultStreamConfig(streamConfig)
        .build();

    when(stub.createBasin(any(CreateBasinRequest.class))).thenReturn(response);

    var result = accountService.createBasin(TEST_BASIN, config);

    assertEquals(TEST_BASIN, result.getName());
    verify(stub).createBasin(argThat(request -> 
        request.getBasin().equals(TEST_BASIN) &&
        request.getConfig().equals(config)
    ));
  }

  @Test
  void deleteBasin_shouldDeleteExistingBasin() {
    accountService.deleteBasin(TEST_BASIN);

    verify(stub).deleteBasin(argThat(request ->
        request.getBasin().equals(TEST_BASIN)
    ));
  }

  @Test
  void reconfigureBasin_shouldUpdateBasinConfig() {
    var streamConfig = StreamConfig.newBuilder()
        .setStorageClass(StorageClass.STORAGE_CLASS_EXPRESS)
        .build();

    var newConfig = BasinConfig.newBuilder()
        .setDefaultStreamConfig(streamConfig)
        .build();

    accountService.reconfigureBasin(TEST_BASIN, newConfig);

    verify(stub).reconfigureBasin(argThat(request ->
        request.getBasin().equals(TEST_BASIN) &&
        request.getConfig().equals(newConfig)
    ));
  }

  @Test
  void createBasin_shouldThrowWhenConfigIsNull() {
    assertThrows(NullPointerException.class, () ->
        accountService.createBasin(TEST_BASIN, null)
    );
  }

  @Test
  void reconfigureBasin_shouldThrowWhenConfigIsNull() {
    assertThrows(NullPointerException.class, () ->
        accountService.reconfigureBasin(TEST_BASIN, null)
    );
  }
}
