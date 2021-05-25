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
    public static final String[] hintHashes = {"1582824a01c4b842e207a51e3cfc47212885e58eb147e33ea29ba212e611904d","e91aca467f5a2a280213a46aa11842d92577322b5c9899c8e6ffb3dc4b1d80a1","52be0093f91b90872aa54533b8ee9b38f794999bae9371834eca23ce51139b99","8052d9420a20dfc6197d514263f7f0d67f1296569f3c0708fa9030b08a4a908a","ca70f765d8c1b9b7a2162b19ea8e2b410166840f67ee276d297d0ab3bc05f425","570d3ada41deeb943fde8f397076eaef39862f12b0496eadee5b090face72eb5","f224061bd0359a5ca697570df620c34ecda0454fde04f511e4c8608b1f19acb8","01d8adcfdb790125e585e7ed9b77741cd09596dd7c15dcfdc75382a31a4f74ad","4b47ac115f6a91120d444638be98a97d009b9c13fa820d66796d2dad30d18975"};
    private static final char crackedHint = 'C';
    private static final String passwordChars = "ABCDEFGHIJK";
    private static final String[] hintsCracked = {"HJKGDEFBIC","FCJADEKGHI","FAJBDIEKGH","AGCJEHFKIB","BHKICGFADJ","JIFAGKDBCE","GAHDKJBCEF","EBIKHGDAFC","DJHAFGICBE"};
    private static final char[] hintChars = {'A', 'B', 'C', 'D', 'E', 'H', 'I', 'J', 'K'};

    static class TestActor extends AbstractLoggingActor {

        public static Props props(ActorRef parent) {
            return Props.create(HintCrackingWorkerTest.TestActor.class, () -> new HintCrackingWorkerTest.TestActor(parent));
        }

        public TestActor(ActorRef parent) {
            this.parent = parent;
        }

        ActorRef parent = null;
        ActorRef hintCrackingWorker = this.createHintCrackingWorker(hintHashes, passwordChars);

        public ActorRef createHintCrackingWorker(String[] hintHashes, String passwordChars) {
            ActorRef worker = this.context().actorOf(HintCrackingWorker.props(hintHashes, passwordChars), "hintCrackingWorker");
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
                    for(int i = 0; i < HintCrackingWorkerTest.hintsCracked.length; i++) {
                        PasswordCrackingWorker.HintCrackedMessage expectedMessage = new PasswordCrackingWorker.HintCrackedMessage(hintChars[i], i, hintsCracked[i]);
                        this.expectMsg(expectedMessage);
                    }

                    // Will wait for the rest of the within duration
                    expectNoMessage();
                    return null;
                });
            }
        };
    }
}
