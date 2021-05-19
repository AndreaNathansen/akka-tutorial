package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.singletons.ConfigurationSingleton;
import de.hpi.ddm.systems.MasterSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

public class PasswordCrackingWorkerTest {
    static ActorSystem system;
    private static final String hintHash = "1582824a01c4b842e207a51e3cfc47212885e58eb147e33ea29ba212e611904d";
    private static final char crackedHint = 'A';
    private static final String passwordChars = "ABCDEFGHIJK";

    static class TestActor extends AbstractLoggingActor {

        public static Props props(ActorRef parent) {
            return Props.create(PasswordCrackingWorkerTest.TestActor.class, () -> new PasswordCrackingWorkerTest.TestActor(parent));
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
                within(Duration.ofSeconds(10), () -> {
                    ActorRef testActor = system.actorOf(PasswordCrackingWorkerTest.TestActor.props(this.getRef()), "testActor");
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
