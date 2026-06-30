package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteResult;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;

public class Replica extends AbstractReplica {

  private final int[] positions = new int[POSITIONS_LIST_LENGTH];
  private final Random rnd = new Random();
  private final Map<Long, Cancellable> forwardTimers = new HashMap<>();
  private final Map<UpdateId, Cancellable> writeOkTimers = new HashMap<>();
  // Updates received via UPDATE but not yet committed (awaiting WRITEOK).
  private final Map<UpdateId, Update> proposed = new HashMap<>();
  // Updates already applied to local state (also serves as the dedup set / history).
  private final Map<UpdateId, Update> history = new HashMap<>();
  // Writes this replica was contacted for and still owes an answer to the client.
  private final Map<Long, PendingWrite> pendingWrites = new HashMap<>();
  // Coordinator-side quorum tracking: distinct ackers per update.
  private final Map<UpdateId, Set<ActorRef>> ackers = new HashMap<>();
  private final Set<UpdateId> writeOkSent = new HashSet<>();
  private final Set<Integer> knownCrashed = new HashSet<>();
  // Coordinator-side epoch/sequence allocation for new updates.
  private int epoch = 0;
  private Cancellable electionAckTimer = null;
  private Cancellable electionGlobalTimer = null;
  private boolean participating = false;
  private Cancellable heartbeatBeatTimer = null;    // coordinator: periodic beat
  private Cancellable heartbeatTimeoutTimer = null; // replica: coordinator liveness
  private long writeReqCounter = 0;
  private int n; // Number of actors
  private AbstractReplica.Crash pendingCrash = null;
  private int crashCounter = 0;
  private boolean crashed = false;
  private Map<Integer, ActorRef> group;
  private List<Integer> ringIds;
  private int coordinatorId;
  private boolean isCoordinator;
  // Most recent update applied (used by the election protocol).
  private UpdateId lastUpdate = null;
  private int nextSeq = 0;
  private int activeInitiator = -1;          // highest initiator id we are forwarding
  private UpdateId frozenLastUpdate = null;  // Hint 1: frozen while participating
  private Election electionInFlight = null;  // last ELECTION we forwarded (for retries)
  private int electionAckTarget = -1;
  private boolean awaitingElectionAck = false;

  public Replica(int id) {
    this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
        AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
  }

  public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
      Optional<ActorRef> listener) {
    super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
  }

  public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
    return Props.create(Replica.class,
        () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
  }

  // Props method for automated tests
  public static Props propsWithListener(int id, int minLatency, int maxLatency,
      int coordinatorBeatInterval, ActorRef listener) {
    return Props.create(Replica.class,
        () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval,
            Optional.ofNullable(listener)));
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

  /**
   * Applies a committed update to local state (idempotently) and, if this replica was the one
   * contacted by the client, answers that client.
   */
  private void applyUpdate(Update u) {
    if (history.containsKey(u.id)) {
      return; // never deliver the same update twice
    }
    proposed.remove(u.id);
    cancel(writeOkTimers.remove(u.id));
    positions[u.index] = u.value;
    history.put(u.id, u);
    if (lastUpdate == null || u.id.compareTo(lastUpdate) > 0) {
      lastUpdate = u.id;
    }
    log("applied update " + u.id + " (" + u.index + ", " + u.value + ")");
    callbackOnUpdateApplied(u.index, u.value);
    if (u.originId == this.id && u.client != null) {
      this.tell(new WriteResult(true, u.index, u.value, this.id), u.client);
      pendingWrites.remove(u.reqId);
      cancel(forwardTimers.remove(u.reqId));
    }
  }

  /**
   * Coordinator: allocate an id for the write and start phase 1.
   */
  private void coordinatorBeginUpdate(ActorRef client, int originId, int index, int value,
      long reqId) {
    UpdateId uid = new UpdateId(epoch, nextSeq++);
    Update u = new Update(uid, index, value, originId, client, reqId);
    proposed.put(uid, u);
    ackers.put(uid, new HashSet<>());
    debug("coordinator proposing update " + uid + " (" + index + ", " + value + ")");
    broadcast(u);
  }

  /**
   * Send a buffered write to the current coordinator (or process it if we are it).
   */
  private void dispatchWrite(long reqId) {
    PendingWrite pw = pendingWrites.get(reqId);
    if (pw == null) {
      return;
    }
    if (isCoordinator) {
      coordinatorBeginUpdate(pw.client, this.id, pw.index, pw.value, pw.reqId);
    } else {
      this.tell(new WriteForward(pw.client, this.id, pw.index, pw.value, pw.reqId),
          group.get(coordinatorId));
      cancel(forwardTimers.remove(reqId));
      forwardTimers.put(reqId, schedule(updateTimeoutDelay(), new ForwardTimeout(reqId)));
    }
  }

  private void onClientWrite(ClientWrite m) {
    debug("WRITE (" + m.index + ", " + m.value + ") from client " + m.client.path().name());
    long reqId = ++writeReqCounter;
    pendingWrites.put(reqId, new PendingWrite(reqId, m.client, m.index, m.value));
    if (participating) {
      return; // Election in progress: buffer the write and resume once a coordinator exists.
    }
    dispatchWrite(reqId);
  }

  private void onWriteForward(WriteForward f) {
    if (!isCoordinator || participating) {
      return; // not the coordinator (anymore): ignore stale forward
    }
    coordinatorBeginUpdate(f.client, f.originId, f.index, f.value, f.reqId);
  }

  /**
   * Every replica: store the proposed update and acknowledge the coordinator.
   */
  private void onUpdate(Update u) {
    if (participating) {
      return; // ignore stragglers while electing
    }
    if (crashTriggered(AbstractReplica.Crash.Type.Update)) {
      return; // crash "after receiving an UPDATE": do not ack
    }
    proposed.put(u.id, u);
    this.tell(new Ack(u.id), getSender());
    if (u.originId == this.id) {
      PendingWrite pw = pendingWrites.get(u.reqId);
      if (pw != null) {
        pw.assignedId = u.id; // the coordinator has taken charge of our write
        cancel(forwardTimers.remove(u.reqId));
      }
    }
    if (!isCoordinator) {
      cancel(writeOkTimers.remove(u.id));
      writeOkTimers.put(u.id, schedule(updateTimeoutDelay(), new WriteOkTimeout(u.id)));
    }
  }

  /**
   * Coordinator: count acks and broadcast WRITEOK once a quorum is reached.
   */
  private void onAck(Ack a) {
    if (participating) {
      return;
    }
    Set<ActorRef> s = ackers.get(a.id);
    if (s == null) {
      return; // update already confirmed
    }
    s.add(getSender());
    if (s.size() >= quorum() && !writeOkSent.contains(a.id)) {
      writeOkSent.add(a.id);
      ackers.remove(a.id);
      debug("quorum reached for " + a.id + ", broadcasting WRITEOK");
      broadcast(new WriteOk(a.id));
    }
  }

  /**
   * Every replica: apply the update (once) and, if origin, answer the client.
   */
  private void onWriteOk(WriteOk w) {
    if (crashTriggered(AbstractReplica.Crash.Type.WriteOK)) {
      return; // crash during/after WRITEOK dissemination
    }
    Update u = proposed.get(w.id);
    if (u == null) {
      return; // unknown / already applied
    }
    applyUpdate(u);
  }

  private void startHeartbeatBeating() {
    cancel(heartbeatTimeoutTimer);
    heartbeatTimeoutTimer = null;
    cancel(heartbeatBeatTimer);
    heartbeatBeatTimer = schedulePeriodic(getCoordinatorBeatInterval(),
        getCoordinatorBeatInterval(), new HeartbeatTick());
  }

  private void startHeartbeatMonitoring() {
    cancel(heartbeatBeatTimer);
    heartbeatBeatTimer = null;
    resetHeartbeatTimeout();
  }

  private void resetHeartbeatTimeout() {
    cancel(heartbeatTimeoutTimer);
    heartbeatTimeoutTimer = schedule(heartbeatTimeoutDelay(), new HeartbeatTimeout());
  }

  private void onHeartbeatTick(HeartbeatTick t) {
    if (!isCoordinator) {
      return;
    }
    if (crashTriggered(AbstractReplica.Crash.Type.Heartbeat)) {
      return; // coordinator crashes after sending heartbeats
    }
    broadcastToOthers(new Heartbeat());
  }

  private void onHeartbeat(Heartbeat h) {
    if (isCoordinator || participating) {
      return;
    }
    resetHeartbeatTimeout();
  }

  private void onHeartbeatTimeout(HeartbeatTimeout t) {
    if (crashed || participating || isCoordinator) {
      return;
    }
    log("coordinator " + coordinatorId + " suspected (no heartbeat)");
    startElection();
  }

  private void onForwardTimeout(ForwardTimeout t) {
    forwardTimers.remove(t.reqId);
    if (crashed || participating) {
      return;
    }
    PendingWrite pw = pendingWrites.get(t.reqId);
    if (pw == null || pw.assignedId != null) {
      return; // already taken charge of
    }
    log("coordinator " + coordinatorId + " suspected (no UPDATE after forward)");
    startElection();
  }

  private void onWriteOkTimeout(WriteOkTimeout t) {
    writeOkTimers.remove(t.id);
    if (crashed || participating) {
      return;
    }
    if (history.containsKey(t.id) || !proposed.containsKey(t.id)) {
      return; // already applied
    }
    log("coordinator " + coordinatorId + " suspected (no WRITEOK after UPDATE)");
    startElection();
  }


  /**
   * Next replica in the ascending-id ring, skipping ids known to have crashed.
   */
  private int successor() {
    int idx = ringIds.indexOf(this.id);
    for (int k = 1; k <= ringIds.size(); k++) {
      int cand = ringIds.get((idx + k) % ringIds.size());
      if (cand == this.id) {
        break;
      }
      if (!knownCrashed.contains(cand)) {
        return cand;
      }
    }
    return -1; // everyone else is believed crashed
  }

  private void enterElection(int crashedCoord) {
    participating = true;
    activeInitiator = -1;
    cancel(heartbeatTimeoutTimer);
    heartbeatTimeoutTimer = null;
    knownCrashed.add(crashedCoord);
    frozenLastUpdate = lastUpdate; // Hint 1: freeze our "most recent update"
    callbackOnElectionStarted(crashedCoord);
    cancel(electionGlobalTimer);
    electionGlobalTimer = schedule(electionGlobalTimeoutDelay(), new ElectionTimeout());
  }

  /**
   * Begin a fresh election because we suspect the current coordinator.
   */
  private void startElection() {
    if (crashed || participating) {
      return;
    }
    enterElection(coordinatorId);
    activeInitiator = this.id;
    List<Candidate> cands = new ArrayList<>();
    cands.add(new Candidate(this.id, frozenLastUpdate));
    sendElectionToSuccessor(new Election(this.id, coordinatorId, cands, false, -1));
  }

  private void sendElectionToSuccessor(Election e) {
    int suc = successor();
    if (suc < 0) {
      // We are the only survivor we know of: decide right away.
      int winner = computeWinner(e.candidates);
      if (winner == this.id) {
        declareVictory();
      }
      return;
    }
    cancel(electionAckTimer);
    electionInFlight = e;
    electionAckTarget = suc;
    awaitingElectionAck = true;
    this.tell(e, group.get(suc));
    electionAckTimer = schedule(electionAckTimeoutDelay(), new ElectionAckTimeout(suc));
  }

  private void onElection(Election e) {
    // Always acknowledge the forwarder so it does not skip us.
    this.tell(new ElectionAck(), getSender());
    if (crashed) {
      return;
    }
    // Stale election about a coordinator we have already replaced.
    if (!participating && coordinatorId != e.crashedCoordinatorId) {
      return;
    }
    if (!participating) {
      enterElection(e.crashedCoordinatorId);
    }
    // Among concurrent elections, keep only the one with the highest initiator id.
    if (e.initiatorId < activeInitiator) {
      return;
    }
    activeInitiator = e.initiatorId;

    if (e.decided) {
      if (e.winnerId == this.id) {
        if (!(isCoordinator && coordinatorId == this.id)) {
          declareVictory();
        }
      } else {
        sendElectionToSuccessor(e);
      }
      return;
    }

    boolean present = false;
    for (Candidate c : e.candidates) {
      if (c.id == this.id) {
        present = true;
        break;
      }
    }

    if (e.initiatorId == this.id && present) {
      // The message has gone all the way around the ring: decide the winner.
      int winner = computeWinner(e.candidates);
      debug("election complete, winner=" + winner);
      if (winner == this.id) {
        declareVictory();
      } else {
        sendElectionToSuccessor(
            new Election(e.initiatorId, e.crashedCoordinatorId, e.candidates, true, winner));
      }
    } else if (present) {
      sendElectionToSuccessor(e);
    } else {
      List<Candidate> cands = new ArrayList<>(e.candidates);
      cands.add(new Candidate(this.id, frozenLastUpdate));
      sendElectionToSuccessor(
          new Election(e.initiatorId, e.crashedCoordinatorId, cands, false, -1));
    }
  }

  private void onElectionAck(ElectionAck a) {
    if (awaitingElectionAck && getSender().equals(group.get(electionAckTarget))) {
      awaitingElectionAck = false;
      cancel(electionAckTimer);
    }
  }

  private void onElectionAckTimeout(ElectionAckTimeout t) {
    if (crashed || !participating || !awaitingElectionAck || t.target != electionAckTarget) {
      return;
    }
    // The ring successor did not answer: assume it crashed, skip it, try the next one.
    knownCrashed.add(t.target);
    awaitingElectionAck = false;
    if (electionInFlight != null) {
      sendElectionToSuccessor(electionInFlight);
    }
  }

  private void onElectionTimeout(ElectionTimeout t) {
    if (crashed || !participating) {
      return;
    }
    // The election did not converge (e.g. the best candidate crashed): restart it.
    log("election timed out, restarting");
    participating = false;
    awaitingElectionAck = false;
    cancel(electionAckTimer);
    startElection();
  }

  /**
   * Picks the most up-to-date alive candidate, breaking ties by highest id.
   */
  private int computeWinner(List<Candidate> cands) {
    Candidate best = null;
    for (Candidate c : cands) {
      if (knownCrashed.contains(c.id)) {
        continue;
      }
      if (best == null || isBetter(c, best)) {
        best = c;
      }
    }
    return best == null ? this.id : best.id;
  }

  private boolean isBetter(Candidate a, Candidate b) {
    int cmp = compareUpdate(a.lastUpdate, b.lastUpdate);
    if (cmp != 0) {
      return cmp > 0;
    }
    return a.id > b.id;
  }

  private int compareUpdate(UpdateId x, UpdateId y) {
    if (x == null && y == null) {
      return 0;
    }
    if (x == null) {
      return -1;
    }
    if (y == null) {
      return 1;
    }
    return x.compareTo(y);
  }

  private void declareVictory() {
    participating = false;
    awaitingElectionAck = false;
    cancel(electionAckTimer);
    cancel(electionGlobalTimer);

    this.isCoordinator = true;
    this.coordinatorId = this.id;

    // Hint 3 (uniform agreement): complete any update we observed without a WRITEOK
    // so that nothing a quorum may have applied is lost.
    List<Update> incomplete = new ArrayList<>(proposed.values());
    incomplete.sort(Comparator.comparing(x -> x.id));
    for (Update u : incomplete) {
      applyUpdate(u);
    }
    proposed.clear();

    // Open a new epoch.
    int base = (lastUpdate == null) ? 0 : lastUpdate.epoch;
    this.epoch = base + 1;
    this.nextSeq = 0;

    log("won election, becoming coordinator (epoch " + epoch + ")");
    callbackOnCoordinatorElected(this.id);

    List<Update> hist = new ArrayList<>(history.values());
    hist.sort(Comparator.comparing(x -> x.id));
    broadcastToOthers(new Synchronization(this.id, this.epoch, hist));

    startHeartbeatBeating();

    // Resume client writes that were waiting for a coordinator.
    for (PendingWrite pw : new ArrayList<>(pendingWrites.values())) {
      if (pw.assignedId == null) {
        coordinatorBeginUpdate(pw.client, this.id, pw.index, pw.value, pw.reqId);
      }
    }
  }

  private void onSynchronization(Synchronization s) {
    if (crashed) {
      return;
    }
    participating = false;
    awaitingElectionAck = false;
    cancel(electionAckTimer);
    cancel(electionGlobalTimer);

    this.coordinatorId = s.newCoordinatorId;
    this.isCoordinator = (this.id == s.newCoordinatorId);
    this.epoch = s.newEpoch;
    this.nextSeq = 0;

    // Converge to the new coordinator's state.
    proposed.clear();
    List<Update> incoming = new ArrayList<>(s.history);
    incoming.sort((x, y) -> x.id.compareTo(y.id));
    for (Update u : incoming) {
      applyUpdate(u);
    }

    log("synchronized with new coordinator " + s.newCoordinatorId + " (epoch " + epoch + ")");
    callbackOnCoordinatorElected(s.newCoordinatorId);

    startHeartbeatMonitoring();

    // Re-issue any client writes still pending against the new coordinator.
    for (PendingWrite pw : new ArrayList<>(pendingWrites.values())) {
      if (pw.assignedId == null) {
        dispatchWrite(pw.reqId);
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
    debug("initialised: n=" + n + " coordinator=" + coordinatorId + " isCoordinator="
        + isCoordinator);
    if (isCoordinator) {
      startHeartbeatBeating();
    } else {
      startHeartbeatMonitoring();
    }
  }


  private Cancellable schedule(long delayMillis, Serializable msg) {
    return getContext().system().scheduler()
        .scheduleOnce(Duration.create(Math.max(1, delayMillis), TimeUnit.MILLISECONDS), getSelf(),
            msg, getContext().system().dispatcher(), getSelf());
  }

  private Cancellable schedulePeriodic(long initialMillis, long intervalMillis, Serializable msg) {
    return getContext().system().scheduler()
        .scheduleWithFixedDelay(Duration.create(Math.max(1, initialMillis), TimeUnit.MILLISECONDS),
            Duration.create(Math.max(1, intervalMillis), TimeUnit.MILLISECONDS), getSelf(), msg,
            getContext().system().dispatcher(), getSelf());
  }

  private long heartbeatTimeoutDelay() {
    // ~2 missed beats + network tolerance, with a little jitter so that not all
    // replicas suspect the coordinator at exactly the same instant.
    return 2L * getCoordinatorBeatInterval() + getMaxLatencyPlusTolerance() + rnd.nextInt(
        getMaxLatency() * Math.max(1, n) + 1);
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
    return createBaseReceiveBuilder().match(ClientRead.class, this::onClientRead)
        .match(ClientWrite.class, this::onClientWrite)
        .match(WriteForward.class, this::onWriteForward).match(Update.class, this::onUpdate)
        .match(Ack.class, this::onAck).match(WriteOk.class, this::onWriteOk)
        .match(HeartbeatTick.class, this::onHeartbeatTick).match(Heartbeat.class, this::onHeartbeat)
        .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
        .match(ForwardTimeout.class, this::onForwardTimeout)
        .match(WriteOkTimeout.class, this::onWriteOkTimeout).match(Election.class, this::onElection)
        .match(ElectionAck.class, this::onElectionAck)
        .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
        .match(ElectionTimeout.class, this::onElectionTimeout)
        .match(Synchronization.class, this::onSynchronization).build();
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
  record WriteForward(ActorRef client, int originId, int index, int value, long reqId) implements
      Serializable {

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
  record Election(int initiatorId, int crashedCoordinatorId, List<Candidate> candidates,
                  boolean decided, int winnerId) implements Serializable {

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
  record Synchronization(int newCoordinatorId, int newEpoch, List<Update> history) implements
      Serializable {

  }

  /**
   * Immutable identifier of an update: the pair {@code <epoch, sequence>}. Used as a map key for
   * the update history and to compare recency.
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
  record Update(UpdateId id, int index, int value, int originId, ActorRef client,
                long reqId) implements Serializable {

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
