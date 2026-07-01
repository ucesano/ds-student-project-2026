package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

public class Client extends AbstractClient {

    private record ReadTimeoutMsg(long reqId,ActorRef replica,int index) implements Serializable {}

    private record WriteTimeoutMsg(long reqId,ActorRef replica,int index,int value) implements Serializable {}

    // A request awaiting an answer from the system.
    private record Pending(long reqId,Cancellable timeout,ActorRef replica) {}

    // Outstanding requests, keyed by the position index they target. Several
    // requests may be in flight for the same index: results do not carry a
    // request id, so they are matched to the OLDEST pending request (FIFO).
    // This is sound because a client interacts with one replica and both the
    // replica's reads and the committed writes are answered in request order.
    private final Map<Integer, Deque<Pending>> pendingReads = new HashMap<>();
    private final Map<Integer, Deque<Pending>> pendingWrites = new HashMap<>();
    private long reqCounter = 0;

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

    // =================================================================================
    // Scheduling helper
    // =================================================================================

    private Cancellable schedule(long delayMillis, Serializable msg) {
        return getContext().system().scheduler().scheduleOnce(
                Duration.create(delayMillis, TimeUnit.MILLISECONDS),
                getSelf(),
                msg,
                getContext().system().dispatcher(),
                getSelf());
    }

    // =================================================================================
    // Sending requests
    // =================================================================================

    @Override
    public void sendRead(ActorRef replica, int index) {
        log("requesting READ (" + index + ") to " + replica.path().name());
        long reqId = ++reqCounter;
        replica.tell(new Replica.ClientRead(getSelf(), index), getSelf());
        Cancellable c = schedule(getReadTimeoutDelay(), new ReadTimeoutMsg(reqId, replica, index));
        pendingReads.computeIfAbsent(index, k -> new ArrayDeque<>()).addLast(new Pending(reqId, c, replica));
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        log("requesting WRITE (" + index + ", " + value + ") to " + replica.path().name());
        long reqId = ++reqCounter;
        replica.tell(new Replica.ClientWrite(getSelf(), index, value), getSelf());
        Cancellable c = schedule(getWriteTimeoutDelay(), new WriteTimeoutMsg(reqId, replica, index, value));
        pendingWrites.computeIfAbsent(index, k -> new ArrayDeque<>()).addLast(new Pending(reqId, c, replica));
    }

    // =================================================================================
    // Receiving answers
    // =================================================================================

    /** Pops the oldest pending request for {@code index}, cancelling its timeout. */
    private static boolean settleOldest(Map<Integer, Deque<Pending>> pending, int index) {
        Deque<Pending> queue = pending.get(index);
        if (queue == null || queue.isEmpty()) {
            return false; // already answered or timed out
        }
        queue.pollFirst().timeout.cancel();
        return true;
    }

    /** Removes the pending request with {@code reqId}, if it is still pending. */
    private static boolean settleById(Map<Integer, Deque<Pending>> pending, int index, long reqId) {
        Deque<Pending> queue = pending.get(index);
        if (queue == null) {
            return false;
        }
        for (Iterator<Pending> it = queue.iterator(); it.hasNext();) {
            if (it.next().reqId == reqId) {
                it.remove();
                return true;
            }
        }
        return false; // stale firing for an already-answered request
    }

    private void onReadResult(ReadResult r) {
        if (settleOldest(pendingReads, r.index)) {
            callbackOnReadResult(r);
        }
    }

    private void onWriteResult(WriteResult r) {
        if (settleOldest(pendingWrites, r.index)) {
            callbackOnWriteResult(r);
        }
    }

    private void onReadTimeoutMsg(ReadTimeoutMsg t) {
        if (settleById(pendingReads, t.index, t.reqId)) {
            callbackOnReadTimeout(new ReadTimeout(getSelf(), t.replica, t.index));
        }
    }

    private void onWriteTimeoutMsg(WriteTimeoutMsg t) {
        if (settleById(pendingWrites, t.index, t.reqId)) {
            callbackOnWriteTimeout(new WriteTimeout(getSelf(), t.replica, t.index, t.value));
        }
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadResult.class, this::onReadResult)
                .match(WriteResult.class, this::onWriteResult)
                .match(ReadTimeoutMsg.class, this::onReadTimeoutMsg)
                .match(WriteTimeoutMsg.class, this::onWriteTimeoutMsg)
                .build();
    }
}
