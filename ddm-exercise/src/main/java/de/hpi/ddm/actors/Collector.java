package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import akka.japi.Pair;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Collector extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "collector";

	public static Props props() {
		return Props.create(Collector.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CollectMessage implements Serializable {
		private static final long serialVersionUID = -102767440935270949L;
		private String result;
		private int lineID;
	}

	@Data
	public static class PrintMessage implements Serializable {
		private static final long serialVersionUID = -267778464637901383L;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private List<Pair<String, Integer>> results = new ArrayList<>();
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CollectMessage.class, this::handle)
				.match(PrintMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(CollectMessage message) {
		this.log().info("Collector received password");
		this.results.add(new Pair<>(message.getResult(), message.getLineID()));
	}
	
	protected void handle(PrintMessage message) {
		this.results.sort(Comparator.comparingInt(Pair::second));
		this.results.forEach(result -> this.log().info("{}", result.first()));
	}
}
