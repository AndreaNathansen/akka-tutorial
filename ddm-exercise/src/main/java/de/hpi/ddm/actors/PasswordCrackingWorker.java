package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.util.Timeout;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.systems.MasterSystem;
import de.hpi.ddm.actors.utils.Util;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import akka.cluster.Member;
import akka.cluster.MemberStatus;

public class PasswordCrackingWorker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";
    private static final int calculationDuration = 1; // 1 second.

    public static Props props() {
        return Props.create(PasswordCrackingWorker.class);
    }

    public PasswordCrackingWorker() {
        this.cluster = Cluster.get(this.context().system());
        // TODO: create unique proxy (= with unique name) for each worker
        this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskCrackPasswordMessage implements Serializable {
        private static final long serialVersionUID = 100000000000000L;
        private int lineID;
        private String passwordChars;
        private int passwordLength;
        private String password;
        private String[] hints;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WelcomeMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private BloomFilter welcomeData;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintCrackedMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private char hint;
    }

    @Data
    @NoArgsConstructor
    private static class ContinueCrackingMessage implements Serializable {
        private static final long serialVersionUID = 889237489237984798L;
    }

    enum YieldState {
        YIELDED,
        COULD_NOT_CRACK,
        CRACKED
    }

    /////////////////
    // Actor State //
    /////////////////

    private Member masterSystem;
    private final Cluster cluster;
    private final ActorRef largeMessageProxy;
    private ActorRef masterRef;
    private long registrationTime;
    private int lineID;
    private String passwordChars;
    private int passwordLength;
    private String password;
    private String[] hints;
    private int numHintsCracked = 0;
    private String crackedPassword = null;

    // Algorithm state
    private char[] currentCombination = null;
    private int[] currentPositions = null;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);

        this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    private void startCracking() {
        numHintsCracked = 0;
        for (int i = 0; i < hints.length; i++) {
            this.log().info("Starting HintCrackingWorker: " + HintCrackingWorker.DEFAULT_NAME + "_" + lineID + "_" + i);
            this.context().actorOf(HintCrackingWorker.props(hints[i], passwordChars), HintCrackingWorker.DEFAULT_NAME + "_" + lineID + "_" + i);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, this::handle)
                .match(MemberUp.class, this::handle)
                .match(MemberRemoved.class, this::handle)
                .match(WelcomeMessage.class, this::handle)
                .match(HintCrackedMessage.class, this::handle)
                .match(TaskCrackPasswordMessage.class, this::handle)
                .match(ContinueCrackingMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(CurrentClusterState message) {
        message.getMembers().forEach(member -> {
            if (member.status().equals(MemberStatus.up()))
                this.register(member);
        });
    }

    private void handle(PasswordCrackingWorker.ContinueCrackingMessage uselessMessage) {
        this.startPasswordCracking();
    }

    private void handle(MemberUp message) {
        this.register(message.member());
    }

    private void resetState(){
        currentCombination = null;
        currentPositions = null;
        passwordChars = null;
        password = null;
        hints = null;
    }

    private void handle(TaskCrackPasswordMessage message) {
        this.log().info("Start cracking password");
        resetState();
        lineID = message.lineID;
        passwordChars = message.passwordChars;
        passwordLength = message.passwordLength;
        password = message.password;
        hints = message.hints;
        crackedPassword = null;
        startCracking();
    }

    private ActorRef getMasterActorRef() {
        Duration timeout = Duration.ofSeconds(600);
        if (masterRef == null) {
            try {
                masterRef = this.getContext().actorSelection(masterSystem.address() + "/user/" + Master.DEFAULT_NAME).resolveOne(timeout).toCompletableFuture().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                throw new IllegalStateException("Could not connect to Master.");
            }
        }
        return masterRef;
    }

    private void register(Member member) {
        if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
            this.masterSystem = member;

            this.getMasterActorRef()
                    .tell(new Master.RegistrationMessage(), this.self());

            this.registrationTime = System.currentTimeMillis();
        }
    }

    private void handle(MemberRemoved message) {
        if (this.masterSystem.equals(message.member()))
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private void handle(HintCrackedMessage message) {
        numHintsCracked++;
        passwordChars = passwordChars.replace("" + message.hint, "");

        if (numHintsCracked == hints.length) {
            this.startPasswordCracking();
        }
    }

    private void handle(WelcomeMessage message) {
        final long transmissionTime = System.currentTimeMillis() - this.registrationTime;
        this.log().info("WelcomeMessage with " + message.getWelcomeData().getSizeInMB() + " MB data received in " + transmissionTime + " ms.");
    }

    private void startPasswordCracking() {
        YieldState yieldState = iteratePasswordForTaubeNuesschen();
        switch (yieldState) {
            case YIELDED:
                this.self().tell(new ContinueCrackingMessage(), this.self());
                break;
            case CRACKED:
                Master.PasswordCrackedMessage passwordCrackedMessage = new Master.PasswordCrackedMessage(crackedPassword, lineID, this.self());
                //this.getMasterActorRef().tell(passwordCrackedMessage, this.self());
                this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(passwordCrackedMessage, this.getMasterActorRef()), this.self());
                resetState();
                break;
            case COULD_NOT_CRACK:
                this.log().error("Could not crack password: " + password + "!");
                throw new IllegalStateException("Could not crack password: " + password + "!");
        }
    }

    // original: https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
    // The method that prints all
    // possible strings of length k.
    // It is mainly a wrapper over
    // recursive function printAllKLengthRec()

    private YieldState iteratePasswordForTaubeNuesschen() {
        long startTime = Instant.now().getEpochSecond();
        if (crackedPassword != null) {
            return YieldState.CRACKED;
        }

        // from https://stackoverflow.com/a/31177163

        if (currentCombination == null) {
            // allocate an int array to hold the counts:
            // allocate a char array to hold the current combination:
            // initialize to the first value:
            currentCombination = new char[passwordLength];
            currentPositions = new int[passwordLength];
            for (int i = 0; i < passwordLength; i++) {
                currentCombination[i] = passwordChars.toCharArray()[0];
            }
        }

        int place;
        do {
            // output the current combination:
            String currentCombinationString = new String(currentCombination);
            if (Util.hash(currentCombinationString).equals(password)) {
                crackedPassword = currentCombinationString;
                return YieldState.CRACKED;
            }

            if (Instant.now().getEpochSecond() - startTime > calculationDuration){
                return YieldState.YIELDED;
            }

            // move on to the next combination:
            place = passwordLength - 1;
            while (place >= 0) {
                if (++currentPositions[place] == passwordChars.toCharArray().length) {
                    // overflow, reset to zero
                    currentPositions[place] = 0;
                    currentCombination[place] = passwordChars.toCharArray()[0];
                    place--; // and carry across to the next value
                } else {
                    // no overflow, just set the char value and we're done
                    currentCombination[place] = passwordChars.toCharArray()[currentPositions[place]];
                    break;
                }
            }
        } while (place >= 0);
        return YieldState.COULD_NOT_CRACK;
    }
}