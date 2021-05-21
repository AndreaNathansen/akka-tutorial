package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import de.hpi.ddm.actors.utils.Util;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class HintCrackingWorker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "hint_cracking_worker";
    private static final int calculationDuration = 1; // 1 second.
    private static final String yieldSignalString = "yield";

    public static Props props(String hint, String passwordChars) {
        return Props.create(HintCrackingWorker.class, hint, passwordChars);
    }

    public HintCrackingWorker(String hint, String passwordChars) {
        this.hint = hint;
        this.passwordChars = passwordChars;
        this.getHintCharacterResumable();
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    private class ContinueCrackingMessage implements Serializable {
        private static final long serialVersionUID = 889237489237984798L;
    }

    /////////////////
    // Actor State //
    /////////////////

    private String hint;
    private String passwordChars;
    private long startTime;

    private int currentPasswordIndex;

    private char[] passwordCharacters;
    private char[] currentPasswordChars;
    private char[] permutationArray;

    private int[] heapStack;
    private int heapPermutationIndex;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////


    ////////////////////
    // Actor Behavior //
    ////////////////////

    private void getHintCharacterResumable() {
        startTime = Instant.now().getEpochSecond();
        boolean increasedInt = false;
        if (passwordCharacters == null || currentPasswordChars == null) {
            passwordCharacters = passwordChars.toCharArray();
            currentPasswordChars = new char[passwordCharacters.length - 1];
            currentPasswordIndex = 0;
            increasedInt = true;
        }
        for (; currentPasswordIndex < passwordChars.length(); currentPasswordIndex++) {
            // Copy current chars.
            System.arraycopy(passwordCharacters, 0, currentPasswordChars, 0, currentPasswordIndex);
            System.arraycopy(passwordCharacters, currentPasswordIndex + 1, currentPasswordChars, currentPasswordIndex, currentPasswordChars.length - currentPasswordIndex);
            if(increasedInt) {
                permutationArray = Arrays.copyOf(currentPasswordChars, currentPasswordChars.length);
            }
            // Get permutations and test. Yield when time is up.
            String crackedHint = heapAlgorithmForTaubeNuesschen();
            // Send continue message and stop on yield.
            if (crackedHint == yieldSignalString) {
                //this.log().info("Yielding");
                this.self().tell(new ContinueCrackingMessage(), this.sender());
                return;
            }
            // If hint cracked, send hint and reset state.
            else if (crackedHint != null) {
                char hintCharacter = passwordCharacters[currentPasswordIndex];
                this.log().info("Cracked hint!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! " + hintCharacter);
                this.context().parent().tell(new PasswordCrackingWorker.HintCrackedMessage(hintCharacter), this.self());
                resetState();
                this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
                return;
            }
            heapStack = null;
            increasedInt = true;
        }
        this.log().error("Could not crack hint: " + hint + "!");
        throw new IllegalStateException("Could not Crack Hint: " + hint + "!");
    }

    private void resetState() {
        hint = null;
        passwordChars = null;

        passwordCharacters = null;
        currentPasswordChars = null;
        permutationArray = null;

        heapStack = null;
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ContinueCrackingMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(ContinueCrackingMessage uselessMessage) {
        //this.log().info("Resuming");
        getHintCharacterResumable();
    }

    private String heapAlgorithmForTaubeNuesschen() {
        if (heapStack == null) {
            heapStack = new int[currentPasswordChars.length];
            for (int i = 0; i < currentPasswordChars.length; i++) {
                heapStack[i] = 0;
            }
            heapPermutationIndex = 1;
        }

        String currentPermutation = new String(permutationArray);
        if (testPermutation(currentPermutation)) {
            return currentPermutation;
        }
        while (heapPermutationIndex < currentPasswordChars.length) {
            if (heapStack[heapPermutationIndex] < heapPermutationIndex) {
                int charPos = heapPermutationIndex % 2 == 0 ? 0 : heapStack[heapPermutationIndex];

                char temp = permutationArray[charPos];
                permutationArray[charPos] = permutationArray[heapPermutationIndex];
                permutationArray[heapPermutationIndex] = temp;

                currentPermutation = new String(permutationArray);
                if (testPermutation(currentPermutation)) {
                    return currentPermutation;
                }
                heapStack[heapPermutationIndex]++;
                heapPermutationIndex = 1;
            } else {
                heapStack[heapPermutationIndex] = 0;
                heapPermutationIndex++;
            }
            if (Instant.now().getEpochSecond() - startTime > calculationDuration) {
                return yieldSignalString;
            }
        }
        return null;
    }

    private boolean testPermutation(String permutation) {
        String hashedPermutation = Util.hash(permutation);
        return hashedPermutation.equals(hint);
    }


}
