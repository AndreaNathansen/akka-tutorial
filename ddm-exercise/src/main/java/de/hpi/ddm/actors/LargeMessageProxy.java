package de.hpi.ddm.actors;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Callable;

import akka.actor.*;
import akka.serialization.JavaSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";

	static int messageSize = 6 * 1024 * 1024;
	private int messageIDCounter= 0;
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesPartMessage implements Serializable {
		private static final long serialVersionUID = 10000000000000000L;
		private byte[] bytes;
		private String messageId;
		private int numChunks;
		private int chunkId;
		private ActorRef sender;
		private ActorRef receiver;
	}

	private String getMessageID(){
		return this.self().toString() + this.messageIDCounter++;
	}

	private HashMap<String, byte [][]> receivedBytePartLargeMessages = new HashMap<>();
	private HashMap<String, Integer> numBytesReceived = new HashMap<>();

	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesPartMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> largeMessage) throws IOException {
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		// The following code sends the entire message wrapped in a BytesMessage, which will definitely fail in a distributed setting if the message is large!
		// Solution options:
		// a) Split the message into smaller batches of fixed size and send the batches via ...
		//    a.a) self-build send-and-ack protocol (see Master/Worker pull propagation), or
		//    a.b) Akka streaming using the streams build-in backpressure mechanisms.
		// b) Send the entire message via Akka's http client-server component.
		// c) Other ideas ...
		// Hints for splitting:
		// - To split an object, serialize it into a byte array and then send the byte array range-by-range (tip: try "KryoPoolSingleton.get()").
		// - If you serialize a message manually and send it, it will, of course, be serialized again by Akka's message passing subsystem.
		// - But: Good, language-dependent serializers (such as kryo) are aware of byte arrays so that their serialization is very effective w.r.t. serialization time and size of serialized data.
		byte[] serializedMessage = new byte[0];
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(largeMessage.getMessage());
			oos.flush();
			serializedMessage = bos.toByteArray();
		}
		int chunkID = 0;
		int numChunks =  (int) Math.ceil(serializedMessage.length / (double)messageSize);
		for (int i = 0; i < serializedMessage.length; i += messageSize){
			byte[] msg = Arrays.copyOfRange(serializedMessage, i, Math.min(i + messageSize, serializedMessage.length));
			receiverProxy.tell(new BytesPartMessage(msg, this.getMessageID(), numChunks, chunkID, sender, receiver), this.self());
			chunkID++;
		}
	}

	// Let it crash.
	private void handle(BytesPartMessage message) throws IOException, ClassNotFoundException {
		// New message.
		String messageId = message.getMessageId();
		if (!receivedBytePartLargeMessages.containsKey(messageId)) {
			receivedBytePartLargeMessages.put(messageId, new byte[message.getNumChunks()][]);
			numBytesReceived.put(messageId, 0);
		}
		byte[][] receivedBytePartMessages = receivedBytePartLargeMessages.get(messageId);
		receivedBytePartMessages[message.getChunkId()] = message.getBytes();
		Integer numBytesReceivedForMessage = numBytesReceived.get(messageId);

		numBytesReceivedForMessage++;
		// Received whole message.
		if (numBytesReceivedForMessage == receivedBytePartMessages.length){
			// Get large message byte size.
			int msgSize = 0;
			for (byte[] bytePartMessage : receivedBytePartMessages) {
				msgSize += bytePartMessage.length;
			}
			// Copy two dimensional large message to one dimensional array.
			byte [] largeMessageBytes = new byte[msgSize];
			int index = 0;
			for (byte[] receivedBytePartMessage : receivedBytePartMessages) {
				for (byte b : receivedBytePartMessage) {
					largeMessageBytes[index++] = b;
				}
			}
			// Deserialize object.
			Object sendObject = JavaSerializer.currentSystem().withValue( (ExtendedActorSystem) context().system(), (Callable<Object>) () -> {
				ByteArrayInputStream bis = new ByteArrayInputStream(largeMessageBytes);
				ObjectInputStream ois = new ObjectInputStream(bis);
				return ois.readObject();
			});


			// Reset internal message buffer.
			receivedBytePartLargeMessages.remove(messageId);
			numBytesReceived.remove(messageId);
			// Send large message.
			message.getReceiver().tell(sendObject, message.getSender());
		}
	}
}
