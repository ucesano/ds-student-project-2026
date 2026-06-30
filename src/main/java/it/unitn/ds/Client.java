package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;

public class Client extends AbstractClient {

  private final Map<Integer, Pending> pendingReads = new HashMap<>();
  private final Map<Integer, Pending> pendingWrites = new HashMap<>();
  private long reqCounter = 0;

  Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica,
      Optional<ActorRef> listener) {
    super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
  }

  public static Props props(long readTimeoutDelay, long writeTimeoutDelay,
      Optional<ActorRef> defaultTargetReplica) {
    return Props.create(Client.class,
        () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica,
            Optional.empty()));
  }

  // Props method for automated tests
  public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay,
      Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
    return Props.create(Client.class,
        () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica,
            Optional.ofNullable(listener)));
  }

  private Cancellable schedule(long delayMillis, Serializable msg) {
    return getContext().system().scheduler()
        .scheduleOnce(Duration.create(delayMillis, TimeUnit.MILLISECONDS), getSelf(), msg,
            getContext().system().dispatcher(), getSelf());
  }

  @Override
  public void sendRead(ActorRef replica, int index) {
    log("requesting READ (" + index + ") to " + replica.path().name());
    // Cancel any previous outstanding read for this index before rescheduling
    // (see slide 23: never reschedule onto a live Cancellable).
    Pending old = pendingReads.remove(index);
    if (old != null) {
      old.timeout.cancel();
    }
    long reqId = ++reqCounter;
    replica.tell(new Replica.ClientRead(getSelf(), index), getSelf());
    Cancellable c = schedule(getReadTimeoutDelay(), new ReadTimeoutMsg(reqId, replica, index));
    pendingReads.put(index, new Pending(reqId, c, replica));
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

  private record ReadTimeoutMsg(long reqId, ActorRef replica, int index) implements Serializable {

  }

  private record WriteTimeoutMsg(long reqId, ActorRef replica, int index, int value) implements
      Serializable {

  }
}

}
