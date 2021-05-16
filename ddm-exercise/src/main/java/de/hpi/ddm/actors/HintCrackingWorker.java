package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.remote.EndpointManager;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class HintCrackingWorker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "hint_cracking_worker";

    public static Props props(String hint) {
        return Props.create(HintCrackingWorker.class, hint);
    }

    public HintCrackingWorker(String hint) {
        this.hint = hint;
        this.crackHint();
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    /////////////////
    // Actor State //
    /////////////////

    private String hint;


    /////////////////////
    // Actor Lifecycle //
    /////////////////////


    ////////////////////
    // Actor Behavior //
    ////////////////////

    private void crackHint(){
        // TODO implement crack logic.

        this.context().parent().tell(new PasswordCrackerWorker.HintCrackedMessage('a'), this.self());
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                //.match(ClusterEvent.CurrentClusterState.class, this::handle)
                //.match(ClusterEvent.MemberUp.class, this::handle)
                //.match(ClusterEvent.MemberRemoved.class, this::handle)
                //.match(PasswordCrackerWorker.WelcomeMessage.class, this::handle)
                // TODO: Add further messages here to share work between Master and Worker actors
                //.match(PasswordCrackerWorker.TaskCrackPasswordMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }
}
