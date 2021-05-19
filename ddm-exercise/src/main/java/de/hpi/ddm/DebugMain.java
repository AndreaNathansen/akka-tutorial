package de.hpi.ddm;

import de.hpi.ddm.actors.PasswordCrackingWorker;
import de.hpi.ddm.actors.utils.Util;
import scala.concurrent.java8.FuturesConvertersImpl;

public class DebugMain {

    String crackedPassword = null;
    String password = "GGGFGFFFFG";

    public DebugMain(){
        iterateAllCombinations("GF".toCharArray(), 10);
    }

    public static void main(String[] args) throws Exception {
        new DebugMain();
    }

    // original: https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
    // The method that prints all
    // possible strings of length k.
    // It is mainly a wrapper over
    // recursive function printAllKLengthRec()
    public void iterateAllCombinations(char[] set, int k)
    {
        int n = set.length;
        iterateAllCombinations(set, "", n, k);
    }

    // The main recursive method
    // to print all possible
    // strings of length k
    public void iterateAllCombinations(char[] set,
                                       String prefix,
                                       int n, int k)
    {
        if(crackedPassword != null){
            return;
        }

        // Base case: k is 0,
        // print prefix
        if (k == 0)
        {
            if (Util.hash(prefix).equals(password)){
                crackedPassword = prefix;
            }
            return;
        }

        // One by one add all characters
        // from set and recursively
        // call for k equals to k-1
        for (int i = 0; i < n; ++i)
        {

            // Next character of input added
            String newPrefix = prefix + set[i];

            // k is decreased, because
            // we have added a new character
            iterateAllCombinations(set, newPrefix,
                    n, k - 1);
        }
    }
}
