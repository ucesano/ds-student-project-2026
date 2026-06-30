package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.InitSystem;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Main {

  private static final int N_REPLICAS = 5;
  private static final int COORDINATOR_ID = 0;
  private static final int IDX = 0; // person-of-interest index used throughout the demo

  // Client request timeouts: the write timeout is generous so it can outlast a
  // coordinator crash + election.
  private static final long CLIENT_READ_TIMEOUT = 3_000;
  private static final long CLIENT_WRITE_TIMEOUT = 30_000;

  public static void main(String[] args) {
    banner("START");

    final ActorSystem system = ActorSystem.create("TestMain");

    Logger.setDestinationStdout();
    Logger.setDebugEnabled(true);

    // Create and initialize the replica group
    Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
    for (int i = 0; i < N_REPLICAS; i++) {
      replicas.put(i, system.actorOf(
          Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
              AbstractReplica.COORDINATOR_BEAT_INTERVAL), "Replica_" + i));
    }

    InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
    for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
      entry.getValue().tell(initMsg, ActorRef.noSender());
    }

    try {
      // =====================================================================
      // Scenario 1 — normal operation (two-phase total order broadcast)
      // =====================================================================
      section("Scenario 1: normal writes and reads (coordinator = " + COORDINATOR_ID + ")");
      ActorRef client1 = newClient(system, replicas.get(1), "client1");
      for (int v = 10; v <= 30; v += 10) {
        client1.tell(new WriteRequest(IDX, v), ActorRef.noSender());
        sleep(400);
      }
      client1.tell(new ReadRequest(IDX), ActorRef.noSender());
      sleep(1_000);

      // =====================================================================
      // Scenario 2 — a non-coordinator crashes; the quorum still commits
      // =====================================================================
      section("Scenario 2: non-coordinator (Replica_2) crashes, writes still succeed");
      replicas.get(2).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
      sleep(200);
      ActorRef client2 = newClient(system, replicas.get(3), "client2");
      client2.tell(new WriteRequest(IDX, 42), ActorRef.noSender());
      sleep(800);
      client2.tell(new ReadRequest(IDX), ActorRef.noSender());
      sleep(1_000);

      // =====================================================================
      // Scenario 3 — the coordinator crashes: detection, ring election,
      //              synchronization, and the buffered write is resumed
      // =====================================================================
      section("Scenario 3: coordinator (Replica_0) crashes -> election + recovery");
      replicas.get(COORDINATOR_ID).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
      sleep(200);
      ActorRef client3 = newClient(system, replicas.get(4), "client3");
      // This write is forwarded to the dead coordinator; the contacted replica
      // detects the failure, an election runs, and the write is then committed.
      client3.tell(new WriteRequest(IDX, 99), ActorRef.noSender());
      sleep(6_000);
      client3.tell(new ReadRequest(IDX), ActorRef.noSender());
      sleep(1_500);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      system.terminate();
    }

    system.terminate();

    banner("END");
  }

  private static void banner(String text) {
    System.out.println("\n========================================");
    System.out.println(text);
    System.out.println("========================================\n");
  }

  private static void section(String title) {
    System.out.println("\n----------------------------------------");
    System.out.println(title);
    System.out.println("----------------------------------------");
  }

  private static ActorRef newClient(ActorSystem system, ActorRef target, String name) {
    return system.actorOf(
        Client.props(CLIENT_READ_TIMEOUT, CLIENT_WRITE_TIMEOUT, Optional.of(target)), name);
  }

  private static void sleep(long ms) throws InterruptedException {
    Thread.sleep(ms);
  }
}
