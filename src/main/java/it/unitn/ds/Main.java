package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.InitSystem;

/**
 * Manual demonstration of the protocol through 4 representative scenarios,
 * including the corner cases of a coordinator crashing in the middle of the
 * WRITEOK dissemination (uniform agreement) and of a replica crashing while
 * the ring election is in progress. The protocol's key steps are printed by
 * the {@link Logger}.
 *
 * With N = 9 replicas the quorum is 5, so up to 4 replicas may crash: the demo
 * crashes exactly 4 (replicas 2, 0, 8 and 7), staying within the assumption
 * that a strict majority never fails.
 */
public class Main {

    private static final int N_REPLICAS = 9;
    private static final int COORDINATOR_ID = 0;
    private static final int IDX = 0; // person-of-interest index used throughout the demo

    // Client request timeouts: the write timeout is generous so it can outlast a
    // coordinator crash + election.
    private static final long CLIENT_READ_TIMEOUT = 3_000;
    private static final long CLIENT_WRITE_TIMEOUT = 30_000;

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("TestMain");
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(false);
        section("START");

        // Create and initialize the replica group
        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i, system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                            AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i));
        }
        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        replicas.values().forEach(r -> r.tell(initMsg, ActorRef.noSender()));

        try {
            // =====================================================================
            // Scenario 1: normal operation (two-phase total order broadcast)
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
            // Scenario 2: a non-coordinator crashes; the quorum still commits
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
            // Scenario 3: the coordinator crashes DURING the WRITEOK dissemination:
            //             only a minority applies the update before the failure.
            //             The election picks the most up-to-date replica, which
            //             completes the interrupted update (uniform agreement).
            // =====================================================================
            section("Scenario 3: coordinator crashes while disseminating WRITEOK -> uniform agreement");
            replicas.get(COORDINATOR_ID).tell(new Crash(Crash.Type.WriteOK, 1), ActorRef.noSender());
            ActorRef client3 = newClient(system, replicas.get(5), "client3");
            client3.tell(new WriteRequest(IDX, 77), ActorRef.noSender());
            sleep(8_000); // crash detection + ring election + synchronization
            client3.tell(new ReadRequest(IDX), ActorRef.noSender());
            sleep(1_000);

            // =====================================================================
            // Scenario 4: the new coordinator (Replica_8) crashes with a write in
            //             flight, and Replica_7 crashes while the election ring is
            //             running: the ring skips it, elects the next-best replica,
            //             and the buffered write is recovered in the new epoch.
            // =====================================================================
            section("Scenario 4: new coordinator crashes with a pending write + crash during election");
            replicas.get(7).tell(new Crash(Crash.Type.Election, 1), ActorRef.noSender());
            replicas.get(8).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
            sleep(200);
            ActorRef client4 = newClient(system, replicas.get(1), "client4");
            // This write is forwarded to the dead coordinator; the contacted replica
            // detects the failure, an election runs (skipping Replica_7, which dies
            // on its first ELECTION message), and the write is then committed.
            client4.tell(new WriteRequest(IDX, 99), ActorRef.noSender());
            sleep(8_000);
            client4.tell(new ReadRequest(IDX), ActorRef.noSender());
            sleep(1_500);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            section("END");
            system.terminate();
        }
    }

    private static ActorRef newClient(ActorSystem system, ActorRef target, String name) {
        return system.actorOf(
                Client.props(CLIENT_READ_TIMEOUT, CLIENT_WRITE_TIMEOUT, Optional.of(target)),
                name);
    }

    private static void section(String title) {
        Logger.log("========== " + title + " ==========");
    }

    private static void sleep(long ms) throws InterruptedException {
        Thread.sleep(ms);
    }
}
