package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Client extends AbstractClient {

  private final Map<Integer, Pending> pendingReads = new HashMap<>();

  Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
    super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
  }

  public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
    return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
  }

  // Props method for automated tests
  public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
    return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
  }

  @Override
  public void sendRead(ActorRef replica, int index) {
    // TODO: implement
  }

  @Override
  public void sendWrite(ActorRef replica, int index, int value) {
    // TODO: implement
  }

  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder()
        // TODO add your message handlers here .match(, )
        .build();
  }

  // A request awaiting an answer from the system.
  private record Pending(long reqId, Cancellable timeout, ActorRef replica) {

  }

}
