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

  @Override
  public void sendRead(ActorRef replica, int index) {
    log("requesting READ (" + index + ") to " + replica.path().name());
    // Cancel any previous outstanding read for this index before rescheduling
    Pending old = pendingReads.remove(index);
    if (old != null) {
      old.timeout.cancel();
    }
    long reqId = ++reqCounter;
    replica.tell(new Replica.ClientRead(getSelf(), index), getSelf());
    Cancellable c = schedule(getReadTimeoutDelay(), new ReadTimeoutMsg(reqId, replica, index));
    pendingReads.put(index, new Pending(reqId, c, replica));
  }

  private Cancellable schedule(long delayMillis, Serializable msg) {
    return getContext().system().scheduler()
        .scheduleOnce(Duration.create(delayMillis, TimeUnit.MILLISECONDS), getSelf(), msg,
            getContext().system().dispatcher(), getSelf());
  }

  @Override
  public void sendWrite(ActorRef replica, int index, int value) {
    log("requesting WRITE (" + index + ", " + value + ") to " + replica.path().name());
    Pending old = pendingWrites.remove(index);
    if (old != null) {
      old.timeout.cancel();
    }
    long reqId = ++reqCounter;
    replica.tell(new Replica.ClientWrite(getSelf(), index, value), getSelf());
    Cancellable c = schedule(getWriteTimeoutDelay(),
        new WriteTimeoutMsg(reqId, replica, index, value));
    pendingWrites.put(index, new Pending(reqId, c, replica));
  }

  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder().match(ReadResult.class, this::onReadResult)
        .match(WriteResult.class, this::onWriteResult)
        .match(ReadTimeoutMsg.class, this::onReadTimeoutMsg)
        .match(WriteTimeoutMsg.class, this::onWriteTimeoutMsg).build();
  }

  private void onReadResult(ReadResult r) {
    Pending p = pendingReads.remove(r.index);
    if (p == null) {
      return; // already answered or timed out
    }
    p.timeout.cancel();
    callbackOnReadResult(r);
  }

  private void onWriteResult(WriteResult r) {
    Pending p = pendingWrites.remove(r.index);
    if (p == null) {
      return;
    }
    p.timeout.cancel();
    callbackOnWriteResult(r);
  }

  private void onReadTimeoutMsg(ReadTimeoutMsg t) {
    Pending p = pendingReads.get(t.index);
    if (p == null || p.reqId != t.reqId) {
      return; // stale firing for an already-answered request
    }
    pendingReads.remove(t.index);
    callbackOnReadTimeout(new ReadTimeout(getSelf(), t.replica, t.index));
  }

  private void onWriteTimeoutMsg(WriteTimeoutMsg t) {
    Pending p = pendingWrites.get(t.index);
    if (p == null || p.reqId != t.reqId) {
      return;
    }
    pendingWrites.remove(t.index);
    callbackOnWriteTimeout(new WriteTimeout(getSelf(), t.replica, t.index, t.value));
  }

  private record ReadTimeoutMsg(long reqId, ActorRef replica, int index) implements Serializable {

  }

  private record WriteTimeoutMsg(long reqId, ActorRef replica, int index, int value) implements
      Serializable {

  }

  // A request awaiting an answer from the system.
  private record Pending(long reqId, Cancellable timeout, ActorRef replica) {

  }

}
