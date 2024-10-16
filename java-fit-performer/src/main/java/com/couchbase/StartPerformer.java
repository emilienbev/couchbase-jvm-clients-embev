package com.couchbase;

import com.couchbase.client.core.logging.LogRedaction;
import com.couchbase.client.core.logging.RedactionLevel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Hooks;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class StartPerformer {
  private static final Logger logger = LoggerFactory.getLogger(JavaPerformer.class);
  public static void main(String[] args) throws IOException, InterruptedException {
    int port = 8060;

    // Better reactor stack traces for low cost
    // Unfortunately we cannot have this without pulling in reactor-tools, which can then pill in an incompatible
    // reactor-core when we are building old versions of the SDK.
    // ReactorDebugAgent.init();

    // Control ultra-verbose logging
    System.setProperty("com.couchbase.transactions.debug.lock", "true");
    System.setProperty("com.couchbase.transactions.debug.monoBridge", "false");

    // Setup global error handlers
    Hooks.onErrorDropped(err -> {
      // We intentionally don't set globalError here.
      // In a reactive chain, if one operation fails, any parallel operations are cancelled.
      // If those operations subsuquently hit an error, it has nowhere to go (as the original
      // chain has already had an error raised on it), and so it's raised on the global error
      // handle (e.g. here).  Though unfortunate, this is standard reactor UX, and shouldn't
      // be regarded as a test failure.
      logger.info("Async hook drop (probably fine, will happen if multiple concurrent operations in a reactor chain fail): {}\n\t{}",
              err.toString(), Arrays.stream(err.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.joining("\n\t")));
    });

    // Blockhound is disabled as it causes an immediate runtime error on Jenkins
//        BlockHound
//                .builder()
//                .blockingMethodCallback(blockingMethod -> {
//                    globalError.set("Blocking method detected: " + blockingMethod);
//                })
//                .install();

    //Need to send parameters in format : port=8060 version=1.1.0 loglevel=all:Info
    for(String parameter : args) {
      switch (parameter.split("=")[0]) {
        case "port":
          port= Integer.parseInt(parameter.split("=")[1]);
          break;
        default:
          logger.warn("Undefined input: {}. Ignoring it",parameter);
      }
    }

    // Force that log redaction has been enabled
    LogRedaction.setRedactionLevel(RedactionLevel.PARTIAL);

    Server server = ServerBuilder.forPort(port)
            .addService(new JavaPerformer())
            .build();
    server.start();
    logger.info("Server Started at {}", server.getPort());
    server.awaitTermination();
  }
}
