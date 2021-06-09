package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.japi.Pair;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import de.hpi.ddm.actors.utils.Util;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HintCrackingWorker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "hint_cracking_worker";
    private static final int calculationDuration = 1; // 1 second.
    private static final String yieldSignalString = "yield";

    public static Props props(String[] hints, String passwordChars) {
        return Props.create(HintCrackingWorker.class, hints, passwordChars);
    }

    public HintCrackingWorker(String[] hints, String passwordChars) {
        this.hints = new ArrayList<>(Arrays.asList(hints));
        this.hintIndicesNotCrackedYet = new ArrayList<>();
        for(int i = 0; i < this.hints.size(); i++){
            hintIndicesNotCrackedYet.add(i);
        }
        this.passwordChars = passwordChars;
        this.getHintCharacterResumable();
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    private static class ContinueCrackingMessage implements Serializable {
        private static final long serialVersionUID = 889237489237984798L;
    }

    /////////////////
    // Actor State //
    /////////////////

    private ArrayList<String> hints;
    private ArrayList<Integer> hintIndicesNotCrackedYet;
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
            if (increasedInt) {
                permutationArray = Arrays.copyOf(currentPasswordChars, currentPasswordChars.length);
            }
            // Get permutations and test. Yield when time is up.
            HintPermutationIndexPair crackedHint = heapAlgorithmForTaubeNuesschen();
            // Send continue message and stop on yield.
            if (crackedHint != null && crackedHint.permutation.equals(yieldSignalString)) {
                // this.log().info("Yielding");
                this.self().tell(new ContinueCrackingMessage(), this.sender());
                return;
            }
            // If hint cracked, send hint and reset state.
            else if (crackedHint != null) {
                char hintCharacter = passwordCharacters[currentPasswordIndex];
                //this.log().info("Cracked hint!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! " + hintCharacter);
                this.context().parent().tell(new PasswordCrackingWorker.HintCrackedMessage(hintCharacter, crackedHint.index, crackedHint.permutation), this.self());
                this.hintIndicesNotCrackedYet.remove(new Integer(crackedHint.index));
            }
            heapStack = null;
            increasedInt = true;
        }
        for(Integer i: hintIndicesNotCrackedYet){
            this.log().error("Could not crack hint: " + hints.get(i) + "!");
            throw new IllegalStateException("Could not Crack Hint: " + hints.get(i) + "!");
        }
        resetState();
        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private void resetState() {
        hints = null;
        passwordChars = null;

        passwordCharacters = null;
        currentPasswordChars = null;
        permutationArray = null;

        heapStack = null;
    }

    private static class HintPermutationIndexPair {
        String permutation;
        int index;

        HintPermutationIndexPair(String permutation, int index) {
            this.permutation = permutation;
            this.index = index;
        }
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

    private HintPermutationIndexPair heapAlgorithmForTaubeNuesschen() {
        if (heapStack == null) {
            heapStack = new int[currentPasswordChars.length];
            for (int i = 0; i < currentPasswordChars.length; i++) {
                heapStack[i] = 0;
            }
            heapPermutationIndex = 1;
        }

        String currentPermutation = new String(permutationArray);
        int result = testPermutation(currentPermutation);
        // cracked hint at index result.
        if (result >= 0) {
            return new HintPermutationIndexPair(currentPermutation, result);
        }
        while (heapPermutationIndex < currentPasswordChars.length) {
            if (heapStack[heapPermutationIndex] < heapPermutationIndex) {
                int charPos = heapPermutationIndex % 2 == 0 ? 0 : heapStack[heapPermutationIndex];

                char temp = permutationArray[charPos];
                permutationArray[charPos] = permutationArray[heapPermutationIndex];
                permutationArray[heapPermutationIndex] = temp;

                currentPermutation = new String(permutationArray);
                result = testPermutation(currentPermutation);
                if (result >= 0) {
                    return new HintPermutationIndexPair(currentPermutation, result);
                }
                heapStack[heapPermutationIndex]++;
                heapPermutationIndex = 1;
            } else {
                heapStack[heapPermutationIndex] = 0;
                heapPermutationIndex++;
            }
            if (Instant.now().getEpochSecond() - startTime > calculationDuration) {
                return new HintPermutationIndexPair(yieldSignalString, -1);
            }
        }
        return null;
    }

    private int testPermutation(String permutation) {
        String hashedPermutation = Util.hash(permutation);
        for (Integer i: hintIndicesNotCrackedYet) {
            if(hashedPermutation.equals(hints.get(i))){
                return i;
            }
        }
        return -1;
    }


}
