package de.hpi.ddm.actors;

import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.singletons.ConfigurationSingleton;
import de.hpi.ddm.systems.MasterSystem;

public class HintCrackingWorkerTest {
    static ActorSystem system;
    private static final String hintHash = "52be0093f91b90872aa54533b8ee9b38f794999bae9371834eca23ce51139b99";
    private static final char crackedHint = 'C';
    private static final String passwordChars = "ABCDEFGHIJK";

    static class TestActor extends AbstractLoggingActor {

        public static Props props(ActorRef parent) {
            return Props.create(HintCrackingWorkerTest.TestActor.class, () -> new HintCrackingWorkerTest.TestActor(parent));
        }

        public TestActor(ActorRef parent) {
            this.parent = parent;
        }

        ActorRef parent = null;
        ActorRef hintCrackingWorker = this.createHintCrackingWorker(hintHash, passwordChars);

        public ActorRef createHintCrackingWorker(String hintHash, String passwordChars) {
            ActorRef worker = this.context().actorOf(HintCrackingWorker.props(hintHash, passwordChars), "hintCrackingWorker");
            return worker;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Object.class, message -> this.parent.tell(message, this.self()))
                    .build();
        }
    }

    @Before
    public void setUp() throws Exception {
        final Configuration c = ConfigurationSingleton.get();

        final Config config = ConfigFactory.parseString(
                "akka.remote.artery.canonical.hostname = \"" + c.getHost() + "\"\n" +
                        "akka.remote.artery.canonical.port = " + c.getPort() + "\n" +
                        "akka.cluster.roles = [" + MasterSystem.MASTER_ROLE + "]\n" +
                        "akka.cluster.seed-nodes = [\"akka://" + c.getActorSystemName() + "@" + c.getMasterHost() + ":" + c.getMasterPort() + "\"]")
                .withFallback(ConfigFactory.load("application"));

        system = ActorSystem.create(c.getActorSystemName(), config);
    }

    @After
    public void tearDown() throws Exception {
        TestKit.shutdownActorSystem(system);
    }

    @Test
    public void testHintCracking() {
        // Tests if a hint is cracked correctly
        new TestKit(system) {
            {
                within(Duration.ofSeconds(200), () -> {
                    ActorRef testActor = system.actorOf(HintCrackingWorkerTest.TestActor.props(this.getRef()), "testActor");
                    PasswordCrackingWorker.HintCrackedMessage expectedMessage = new PasswordCrackingWorker.HintCrackedMessage(crackedHint);
                    this.expectMsg(expectedMessage);

                    // Will wait for the rest of the within duration
                    expectNoMessage();
                    return null;
                });
            }
        };
    }
}
