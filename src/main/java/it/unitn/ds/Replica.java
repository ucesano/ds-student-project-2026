package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.AbstractClient.ReadResult;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;

public class Replica extends AbstractReplica {

  private final int[] positions = new int[POSITIONS_LIST_LENGTH];
  private final Random rnd = new Random();
  private final Map<Long, Cancellable> forwardTimers = new HashMap<>();
  private final Map<UpdateId, Cancellable> writeOkTimers = new HashMap<>();
  private int n; // Number of actors
  private AbstractReplica.Crash pendingCrash = null;
  private int crashCounter = 0;
  private boolean crashed = false;
  private Map<Integer, ActorRef> group;
  private List<Integer> ringIds;
  private int coordinatorId;
  private boolean isCoordinator;
  private final Cancellable electionAckTimer = null;
  private final Cancellable electionGlobalTimer = null;
  private final Cancellable heartbeatBeatTimer = null;    // coordinator: periodic beat
  private final Cancellable heartbeatTimeoutTimer = null; // replica: coordinator liveness

  public Replica(int id) {
    this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
  }

  public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
    super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
  }

  public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
    return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
  }

  // Props method for automated tests
  public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
    return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
  }

  private static void cancel(Cancellable c) {
    if (c != null) {
      c.cancel();
    }
  }

  @Override
  public int getSystemNumberOfActors() {
    return n;
  }

  @Override
  public void crash(AbstractReplica.Crash how_to_crash) {
    if (how_to_crash.type() == AbstractReplica.Crash.Type.Now) {
      goCrashed();
    } else {
      this.pendingCrash = how_to_crash;
      this.crashCounter = 0;
    }
  }

  /**
   * Behavior of a crashed node: it ignores everything and sends nothing.
   */
  private Receive crashedReceive() {
    return receiveBuilder().matchAny(m -> {
    }).build();
  }

  private void cancelAllTimers() {
    cancel(heartbeatBeatTimer);
    cancel(heartbeatTimeoutTimer);
    cancel(electionAckTimer);
    cancel(electionGlobalTimer);
    for (Cancellable c : forwardTimers.values()) {
      cancel(c);
    }
    forwardTimers.clear();
    for (Cancellable c : writeOkTimers.values()) {
      cancel(c);
    }
    writeOkTimers.clear();
  }

  private void goCrashed() {
    this.crashed = true;
    cancelAllTimers();
    getContext().become(crashedReceive());
  }

  private boolean crashTriggered(AbstractReplica.Crash.Type type) {
    if (pendingCrash == null || pendingCrash.type() != type) {
      return false;
    }
    crashCounter++;
    if (crashCounter >= pendingCrash.after_n_messages_of_type()) {
      log("CRASHED");
      goCrashed();
      return true;
    }
    return false;
  }

  private int quorum() {
    return n / 2 + 1;
  }

  private void broadcast(Serializable msg) {
    for (ActorRef r : group.values()) {
      this.tell(msg, r);
    }
  }

  private void broadcastToOthers(Serializable msg) {
    for (Map.Entry<Integer, ActorRef> e : group.entrySet()) {
      if (e.getKey() != this.id) {
        this.tell(msg, e.getValue());
      }
    }
  }

  @Override
  public void initSystem(InitSystem sysInit) {
    this.group = sysInit.group();
    this.coordinatorId = sysInit.coordinator_id();
    this.n = group.size();
    this.isCoordinator = (this.id == this.coordinatorId);
    this.ringIds = new ArrayList<>(group.keySet());
    Collections.sort(this.ringIds);
    debug("initialised: n=" + n + " coordinator=" + coordinatorId + " isCoordinator=" + isCoordinator);
    if (isCoordinator) {
      startHeartbeatBeating();
    } else {
      startHeartbeatMonitoring();
    }
  }

  private Cancellable schedule(long delayMillis, Serializable msg) {
    return getContext().system().scheduler().scheduleOnce(Duration.create(Math.max(1, delayMillis), TimeUnit.MILLISECONDS), getSelf(), msg, getContext().system().dispatcher(), getSelf());
  }

  private Cancellable schedulePeriodic(long initialMillis, long intervalMillis, Serializable msg) {
    return getContext().system().scheduler().scheduleWithFixedDelay(Duration.create(Math.max(1, initialMillis), TimeUnit.MILLISECONDS), Duration.create(Math.max(1, intervalMillis), TimeUnit.MILLISECONDS), getSelf(), msg, getContext().system().dispatcher(), getSelf());
  }

  private void startHeartbeatBeating() {
    // TODO: heartbeat layer
  }

  private void startHeartbeatMonitoring() {
    // TODO: heartbeat layer
  }

  private long heartbeatTimeoutDelay() {
    // ~2 missed beats + network tolerance, with a little jitter so that not all
    // replicas suspect the coordinator at exactly the same instant.
    return 2L * getCoordinatorBeatInterval() + getMaxLatencyPlusTolerance() + rnd.nextInt(getMaxLatency() * Math.max(1, n) + 1);
  }

  private long updateTimeoutDelay() {
    // Comfortably larger than a full 2PC round so an alive coordinator never trips it.
    return 4L * getMaxLatencyPlusTolerance();
  }

  private long electionAckTimeoutDelay() {
    return 2L * getMaxLatencyPlusTolerance();
  }

  private long electionGlobalTimeoutDelay() {
    return 2L * n * electionAckTimeoutDelay() + 2L * getCoordinatorBeatInterval();
  }

  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder()
        // TODO add your message handlers here .match(, )
        .build();
  }

  private void onClientRead(ClientRead m) {
    debug("READ (" + m.index + ") from client " + m.client.path().name());
    this.tell(new ReadResult(true, m.index, positions[m.index], this.id), m.client);
  }

  /**
   * Read request sent by a client to the contacted replica.
   */
  public record ClientRead(ActorRef client, int index) implements Serializable {

  }

  /**
   * Write request sent by a client to the contacted replica.
   */
  public record ClientWrite(ActorRef client, int index, int value) implements Serializable {

  }

  /**
   * Write request forwarded by the contacted replica to the coordinator.
   */
  record WriteForward(ActorRef client, int originId, int index, int value, long reqId) implements Serializable {

  }

  /**
   * Phase-1 acknowledgment from a replica to the coordinator.
   */
  record Ack(UpdateId id) implements Serializable {

  }

  /**
   * Phase-2 confirmation: replicas apply the update on reception.
   */
  record WriteOk(UpdateId id) implements Serializable {

  }

  /**
   * Coordinator self-tick that triggers a heartbeat broadcast.
   */
  static class HeartbeatTick implements Serializable {

  }

  /**
   * Liveness beacon broadcast by the coordinator to all replicas.
   */
  static class Heartbeat implements Serializable {

  }

  /**
   * Replica self-message: the coordinator was not heard for too long.
   */
  static class HeartbeatTimeout implements Serializable {

  }

  /**
   * Contacted-replica self-message: the coordinator did not start the update.
   */
  record ForwardTimeout(long reqId) implements Serializable {

  }

  /**
   * Replica self-message: no WRITEOK arrived after an UPDATE.
   */
  record WriteOkTimeout(UpdateId id) implements Serializable {

  }

  /**
   * Ring election message carrying the candidates and their latest updates.
   */
  record Election(int initiatorId, int crashedCoordinatorId, List<Candidate> candidates, boolean decided, int winnerId) implements Serializable {

  }

  /**
   * Acknowledgment of an ELECTION message to its forwarder.
   */
  static class ElectionAck implements Serializable {

  }

  /**
   * Forwarder self-message: the ring successor did not acknowledge in time.
   */
  record ElectionAckTimeout(int target) implements Serializable {

  }

  /**
   * Self-message: the whole election took too long; restart it (termination).
   */
  static class ElectionTimeout implements Serializable {

  }

  /**
   * New coordinator announcement, carrying any updates needed to converge.
   */
  record Synchronization(int newCoordinatorId, int newEpoch, List<Update> history) implements Serializable {

  }

  /**
   * Immutable identifier of an update: the pair {@code <epoch, sequence>}. Used as a map key for the update history and to compare recency.
   */
  public record UpdateId(int epoch, int seq) implements Serializable, Comparable<UpdateId> {

    @Override
    public int compareTo(UpdateId o) {
      if (this.epoch != o.epoch) {
        return Integer.compare(this.epoch, o.epoch);
      }
      return Integer.compare(this.seq, o.seq);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof UpdateId(int epoch1, int seq1)) {
        return epoch1 == this.epoch && seq1 == this.seq;
      }
      return false;
    }

    @Override
    public String toString() {
      return epoch + ":" + seq;
    }
  }

  /**
   * An update record, broadcast by the coordinator and stored by every replica.
   *
   * @param originId id of the replica contacted by the client
   * @param client   client to answer once the update commits
   * @param reqId    origin-local request id, to match the client request
   */
  record Update(UpdateId id, int index, int value, int originId, ActorRef client, long reqId) implements Serializable {

  }

  /**
   * A candidate in the ring election: a replica and the most recent update it knows.
   */
  record Candidate(int id, UpdateId lastUpdate) implements Serializable {

  }

  /**
   * A client write this replica is responsible for answering (it was contacted).
   */
  private static class PendingWrite {

    final long reqId;
    final ActorRef client;
    final int index;
    final int value;
    UpdateId assignedId; // set once the coordinator has bound the write to an <e,i>

    PendingWrite(long reqId, ActorRef client, int index, int value) {
      this.reqId = reqId;
      this.client = client;
      this.index = index;
      this.value = value;
    }
  }
}
