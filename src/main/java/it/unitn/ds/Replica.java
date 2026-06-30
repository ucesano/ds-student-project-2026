package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Replica extends AbstractReplica {

  private final int[] positions = new int[POSITIONS_LIST_LENGTH];
  private int n; // Number of actors
  private AbstractReplica.Crash pendingCrash = null;
  private int crashCounter = 0;
  private boolean crashed = false;
  private Map<Integer, ActorRef> group;
  private List<Integer> ringIds;
  private int coordinatorId;
  private boolean isCoordinator;

  public Replica(int id) {
    this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
  }

  public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
    super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
    // TODO: implement
  }

  public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
    return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
  }

  // Props method for automated tests
  public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
    return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
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

  private void goCrashed() {
    this.crashed = true;
    // TODO: implement timers (here->cancelAll)
    getContext().become(crashedReceive());
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

  private void startHeartbeatBeating() {
    // TODO: heartbeat layer
  }

  private void startHeartbeatMonitoring() {
    // TODO: heartbeat layer
  }

  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder()
        // TODO add your message handlers here .match(, )
        .build();
  }

  /**
   * Read request sent by a client to the contacted replica.
   */
  public record ClientRead(ActorRef client, int index) implements Serializable {

  }
}
