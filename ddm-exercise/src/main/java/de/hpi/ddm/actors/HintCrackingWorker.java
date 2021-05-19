package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.remote.EndpointManager;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import de.hpi.ddm.actors.utils.Util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import de.hpi.ddm.actors.utils.Util;

public class HintCrackingWorker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "hint_cracking_worker";

    public static Props props(String hint, String passwordChars) {
        return Props.create(HintCrackingWorker.class, hint, passwordChars);
    }

    public HintCrackingWorker(String hint, String passwordChars) {
        this.hint = hint;
        this.passwordChars = passwordChars;
        this.crackHint();
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    /////////////////
    // Actor State //
    /////////////////

    private String hint;
    private String passwordChars;


    /////////////////////
    // Actor Lifecycle //
    /////////////////////


    ////////////////////
    // Actor Behavior //
    ////////////////////

    private char getHintCharacter(){
        char [] passwordCharacters = passwordChars.toCharArray();
        char [] currentPasswordChars = new char[passwordCharacters.length - 1];
        ArrayList<String> permutations = new ArrayList<>();
        for (int i = 0; i < passwordChars.length(); i++){
            // Copy current chars.
            System.arraycopy(passwordCharacters, 0, currentPasswordChars, 0, i);
            System.arraycopy(passwordCharacters, i + 1, currentPasswordChars, i, currentPasswordChars.length - i);
            // Get permutations.
            permutations.clear();
            heapPermutation(currentPasswordChars, currentPasswordChars.length, currentPasswordChars.length, permutations);
            for (String permutation: permutations){
                String hashedPermutation = Util.hash(permutation);
                if (hashedPermutation.equals(hint)){
                    return passwordCharacters[i];
                }
            }
        }
        this.log().error("Could not crack hint: " + hint + "!");
        throw new IllegalStateException("Could not Crack Hint: " + hint + "!");
    }

    private void crackHint(){
        char hintCharacter = getHintCharacter();
        this.log().info("Cracked hint!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! " + hintCharacter);
        this.context().parent().tell(new PasswordCrackingWorker.HintCrackedMessage(hintCharacter), this.self());
        this.log().info("Sent hint!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! " + hintCharacter);
        // TODO: add error handling if cracking failed.
        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
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

    // Generating all permutations of an array using Heap's Algorithm
    // https://en.wikipedia.org/wiki/Heap's_algorithm
    // https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
    private void heapPermutation(char[] a, int size, int n, List<String> l) {
        // If size is 1, store the obtained permutation
        if (size == 1)
            l.add(new String(a));

        for (int i = 0; i < size; i++) {
            heapPermutation(a, size - 1, n, l);

            // If size is odd, swap first and last element
            if (size % 2 == 1) {
                char temp = a[0];
                a[0] = a[size - 1];
                a[size - 1] = temp;
            }

            // If size is even, swap i-th and last element
            else {
                char temp = a[i];
                a[i] = a[size - 1];
                a[size - 1] = temp;
            }
        }
    }
}
