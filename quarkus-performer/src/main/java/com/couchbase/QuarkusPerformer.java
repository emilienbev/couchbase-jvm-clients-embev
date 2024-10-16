package com.couchbase;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.commands.TransactionCommandExecutor;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.protocol.observability.SpanCreateRequest;
import com.couchbase.client.protocol.observability.SpanCreateResponse;
import com.couchbase.client.protocol.observability.SpanFinishRequest;
import com.couchbase.client.protocol.observability.SpanFinishResponse;
import com.couchbase.client.protocol.performer.Caps;
import com.couchbase.client.protocol.performer.PerformerCapsFetchRequest;
import com.couchbase.client.protocol.performer.PerformerCapsFetchResponse;
import com.couchbase.client.protocol.shared.API;
import com.couchbase.client.protocol.shared.ClusterConnectionCloseRequest;
import com.couchbase.client.protocol.shared.ClusterConnectionCloseResponse;
import com.couchbase.client.protocol.shared.ClusterConnectionCreateRequest;
import com.couchbase.client.protocol.shared.ClusterConnectionCreateResponse;
import com.couchbase.client.protocol.shared.DisconnectConnectionsRequest;
import com.couchbase.client.protocol.shared.DisconnectConnectionsResponse;
import com.couchbase.client.protocol.shared.EchoRequest;
import com.couchbase.client.protocol.shared.EchoResponse;
import com.couchbase.client.protocol.streams.CancelRequest;
import com.couchbase.client.protocol.streams.CancelResponse;
import com.couchbase.client.protocol.streams.RequestItemsRequest;
import com.couchbase.client.protocol.streams.RequestItemsResponse;
import com.couchbase.client.protocol.transactions.CleanupSetFetchRequest;
import com.couchbase.client.protocol.transactions.CleanupSetFetchResponse;
import com.couchbase.client.protocol.transactions.ClientRecordProcessRequest;
import com.couchbase.client.protocol.transactions.ClientRecordProcessResponse;
import com.couchbase.client.protocol.transactions.TransactionCleanupAttempt;
import com.couchbase.client.protocol.transactions.TransactionCleanupRequest;
import com.couchbase.client.protocol.transactions.TransactionCreateRequest;
import com.couchbase.client.protocol.transactions.TransactionResult;
import com.couchbase.client.protocol.transactions.TransactionSingleQueryRequest;
import com.couchbase.client.protocol.transactions.TransactionSingleQueryResponse;
import com.couchbase.client.protocol.transactions.TransactionStreamDriverToPerformer;
import com.couchbase.client.protocol.transactions.TransactionStreamPerformerToDriver;
import com.couchbase.utils.ClusterConnection;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;

import javax.annotation.Nullable;

@GrpcService
public class QuarkusPerformer extends JavaPerformer {

  @Override
  @Blocking
  protected SdkCommandExecutor executor(com.couchbase.client.protocol.run.Workloads workloads, Counters counters, API api) {
    return super.executor(workloads, counters, api);
  }

  @Override
  @Blocking
  protected TransactionCommandExecutor transactionsExecutor(com.couchbase.client.protocol.run.Workloads workloads, Counters counters) {
    return super.transactionsExecutor(workloads, counters);
  }

  @Override
  @Blocking
  protected void customisePerformerCaps(PerformerCapsFetchResponse.Builder response) {
    super.customisePerformerCaps(response);
  }

  @Override
  @Blocking
  public void clusterConnectionCreate(ClusterConnectionCreateRequest request,
                                      StreamObserver<ClusterConnectionCreateResponse> responseObserver) {
    super.clusterConnectionCreate(request, responseObserver);
  }

  @Override
  @Blocking
  public void clusterConnectionClose(ClusterConnectionCloseRequest request,
                                     StreamObserver<ClusterConnectionCloseResponse> responseObserver) {
    super.clusterConnectionClose(request, responseObserver);
    var hello = new com.couchbase.client.core.env.VersionAndGitHash();
    var test = Cluster.connect()
  }

  @Override
  @Blocking
  public void transactionCreate(TransactionCreateRequest request,
                                StreamObserver<TransactionResult> responseObserver) {
    super.transactionCreate(request, responseObserver);
  }

  @Override
  @Blocking
  public void echo(EchoRequest request , StreamObserver<EchoResponse> responseObserver){
    super.echo(request, responseObserver);
  }

  @Override
  @Blocking
  public void disconnectConnections(DisconnectConnectionsRequest request, StreamObserver<DisconnectConnectionsResponse> responseObserver) {
    super.disconnectConnections(request, responseObserver);
  }

  @Override
  @Blocking
  public StreamObserver<TransactionStreamDriverToPerformer> transactionStream(
    StreamObserver<TransactionStreamPerformerToDriver> toTest) {
    return super.transactionStream(toTest);
  }

  @Override
  @Blocking
  public void transactionCleanup(TransactionCleanupRequest request,
                                 StreamObserver<TransactionCleanupAttempt> responseObserver) {
    super.transactionCleanup(request, responseObserver);
  }

  @Override
  @Blocking
  public void clientRecordProcess(ClientRecordProcessRequest request,
                                  StreamObserver<ClientRecordProcessResponse> responseObserver) {
    super.clientRecordProcess(request, responseObserver);
  }

  @Override
  @Blocking
  public void transactionSingleQuery(TransactionSingleQueryRequest request,
                                     StreamObserver<TransactionSingleQueryResponse> responseObserver) {
    super.transactionSingleQuery(request, responseObserver);
  }

  @Override
  @Blocking
  public void cleanupSetFetch(CleanupSetFetchRequest request, StreamObserver<CleanupSetFetchResponse> responseObserver) {
    super.cleanupSetFetch(request, responseObserver);
  }

  @Override
  @Blocking
  public void spanCreate(SpanCreateRequest request, StreamObserver<SpanCreateResponse> responseObserver) {
    super.spanCreate(request, responseObserver);
  }

  @Override
  @Blocking
  public void spanFinish(SpanFinishRequest request, StreamObserver<SpanFinishResponse> responseObserver) {
    super.spanFinish(request, responseObserver);
  }

  @Override
  @Blocking
  public void performerCapsFetch(PerformerCapsFetchRequest request, StreamObserver<PerformerCapsFetchResponse> responseObserver) {
    super.performerCapsFetch(request, responseObserver);
  }

  @Override
  @Blocking
  public void run(com.couchbase.client.protocol.run.Request request,
                  StreamObserver<com.couchbase.client.protocol.run.Result> responseObserver) {
    super.run(request, responseObserver);
  }

  @Override
  @Blocking
  public void streamCancel(CancelRequest request, StreamObserver<CancelResponse> responseObserver) {
    super.streamCancel(request, responseObserver);
  }

  @Override
  @Blocking
  public void streamRequestItems(RequestItemsRequest request, StreamObserver<RequestItemsResponse> responseObserver) {
    super.streamRequestItems(request, responseObserver);
  }
}
