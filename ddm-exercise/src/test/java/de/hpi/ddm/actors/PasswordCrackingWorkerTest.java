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
    public static final String[] hintHashs = {"1582824a01c4b842e207a51e3cfc47212885e58eb147e33ea29ba212e611904d","e91aca467f5a2a280213a46aa11842d92577322b5c9899c8e6ffb3dc4b1d80a1","52be0093f91b90872aa54533b8ee9b38f794999bae9371834eca23ce51139b99","8052d9420a20dfc6197d514263f7f0d67f1296569f3c0708fa9030b08a4a908a","ca70f765d8c1b9b7a2162b19ea8e2b410166840f67ee276d297d0ab3bc05f425","570d3ada41deeb943fde8f397076eaef39862f12b0496eadee5b090face72eb5","f224061bd0359a5ca697570df620c34ecda0454fde04f511e4c8608b1f19acb8","01d8adcfdb790125e585e7ed9b77741cd09596dd7c15dcfdc75382a31a4f74ad","4b47ac115f6a91120d444638be98a97d009b9c13fa820d66796d2dad30d18975"};
    public static final int passwordLength = 10;
    public static final String passwordChars = "ABCDEFGHIJK";
    public static final String passwordHash = "c4712866799881ac48ca55bf78a9540b1883ae033b52109169eb784969be09d5";
    public static final String crackedPassword = "GGGFGFFFFG";
    public static final int lineID = 1;

    static class TestActor extends AbstractLoggingActor {

        public static Props props(ActorRef parent) {
            return Props.create(PasswordCrackingWorkerTest.TestActor.class, () -> new PasswordCrackingWorkerTest.TestActor(parent));
        }

        public TestActor(ActorRef parent) {
            this.parent = parent;
            this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
        }

        ActorRef parent;
        ActorRef largeMessageProxy;



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
    public void testPasswordCracking() {
        // Tests if a hint is cracked correctly.
        new TestKit(system) {
            {   ActorRef testActor = system.actorOf(PasswordCrackingWorkerTest.TestActor.props(this.getRef()), Master.DEFAULT_NAME);

                ActorRef passwordCrackingWorker = system.actorOf(PasswordCrackingWorker.props(), "passwordCrackingWorker");

                PasswordCrackingWorker.TaskCrackPasswordMessage message = new PasswordCrackingWorker.TaskCrackPasswordMessage(lineID, passwordChars, passwordLength, passwordHash, hintHashs);
                passwordCrackingWorker.tell(message, testActor);

                within(Duration.ofSeconds(6000), () -> {

                    Master.RegistrationMessage regMessage = new Master.RegistrationMessage();
                    this.expectMsg(regMessage);
                    Master.PasswordCrackedMessage expectedMessage = new Master.PasswordCrackedMessage(PasswordCrackingWorkerTest.crackedPassword, PasswordCrackingWorkerTest.lineID, passwordCrackingWorker);
                    this.expectMsg(expectedMessage);

                    // Will wait for the rest of the within duration
                    expectNoMessage();
                    return null;
                });
            }
        };
    }
}
