package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
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

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class TaskCrackPasswordMessage implements Serializable {
		private static final long serialVersionUID = 100000000000000L;
		private int lineID;
		private String passwordChars;
		private int passwordLength;
		private String password;
		private String[] hints;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class WelcomeMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private BloomFilter welcomeData;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintCrackedMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private char hint;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private final ActorRef largeMessageProxy;
	private long registrationTime;
	private int lineID;
	private String passwordChars;
	private int passwordLength;
	private String password;
	private String[] hints;
	private int numHintsCracked = 0;
	private String crackedPassword = null;
	
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

	private void startCracking(){
		numHintsCracked = 0;
		for(int i = 0; i < hints.length; i++) {
			this.log().info("Starting HintCrackingWorker for hint " + i + " : " + hints[i]);
			this.context().actorOf(HintCrackingWorker.props(hints[i], passwordChars), HintCrackingWorker.DEFAULT_NAME + "_" + lineID + "_" + i);
		}
	}
	// TODO: handle unregistration of HintCrackingWorkers and assign tasks to new worker

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(WelcomeMessage.class, this::handle)
				.match(HintCrackedMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
				.match(TaskCrackPasswordMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void handle(TaskCrackPasswordMessage message){
		this.log().info("Start cracking password");
		lineID = message.lineID;
		passwordChars = message.passwordChars;
		passwordLength = message.passwordLength;
		password = message.password;
		hints = message.hints;
		crackedPassword = null;
		startCracking();
	}

	private ActorSelection getMasterActorRef(){
		return this.getContext()
				.actorSelection(masterSystem.address() + "/user/" + Master.DEFAULT_NAME);
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

	private void startPasswordCracking(){
		iterateAllCombinations(passwordChars.toCharArray(), passwordLength);
		if (crackedPassword == null){
			this.log().error("Could not crack password: " + password + "!");
			throw new IllegalStateException("Could not crack password: " + password + "!");
		}

		this.getMasterActorRef().tell(new Master.PasswordCrackedMessage(crackedPassword, lineID, this.self()), this.self());
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