package emulator;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.File;
import java.io.IOException;

public class Emulator {

  private final Server server;

  public Emulator(int port) {
    File f = null;
    this.server =
        ServerBuilder.forPort(port)
            .addService(new EmulatedStreamService()) // Add your service implementation
            .addService(ProtoReflectionService.newInstance())
            .build();
  }

  public void start() throws IOException {
    server.start();
    System.out.println("Server started on port " + server.getPort());
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.err.println("Shutting down server...");
                  stop();
                }));
  }

  public void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public Server getServer() {
    return server;
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    var server = new Emulator(8080);
    server.start();
    server.blockUntilShutdown();
  }
}
