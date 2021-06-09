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

	//static int messageSize = 6 * 1024 * 1024;
	static int messageSize = 262144 - 10;
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
		private ActorRef sendingProxy;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ReceivedBytesPartMessage implements Serializable {
		private static final long serialVersionUID = 1337L;
		private String messageId;
		private int chunkId;
		private ActorRef sendingProxy;
	}

	private String getMessageID(){
		this.messageIDCounter = (this.messageIDCounter + 1) % Integer.MAX_VALUE;
		return this.self().toString() + this.messageIDCounter;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final HashMap<String, byte [][]> receivedBytePartLargeMessages = new HashMap<>();
	private final HashMap<String, BytesPartMessage[]> msgsToSend = new HashMap<>();

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesPartMessage.class, this::handle)
				.match(ReceivedBytesPartMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	////////////////////
	// Actor Behavior //
	////////////////////

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
		String msgID = this.getMessageID();
		int numChunks =  (int) Math.ceil(serializedMessage.length / (double)messageSize);
		System.out.println("size: " + numChunks);
		BytesPartMessage[] msgs = new BytesPartMessage[numChunks];
		for (int i = 0; i < serializedMessage.length; i += messageSize){
			byte[] msg = Arrays.copyOfRange(serializedMessage, i, Math.min(i + messageSize, serializedMessage.length));
			msgs[chunkID] = new BytesPartMessage(msg, msgID, numChunks, chunkID, sender, receiver, this.self());
			chunkID++;
		}
		receiverProxy.tell(msgs[0], this.self());
		msgsToSend.put(msgID, msgs);
	}

	private void handle(ReceivedBytesPartMessage message){
		BytesPartMessage[] msgs = msgsToSend.get(message.getMessageId());
		if (message.getChunkId() == msgs.length - 1) {
			msgsToSend.remove(message.getMessageId());
		} else {
			message.getSendingProxy().tell(msgs[message.getChunkId() + 1], this.self());
		}

	}

	private void handle(BytesPartMessage message) throws IOException, ClassNotFoundException {
		// New message.
		String messageId = message.getMessageId();
		if (!receivedBytePartLargeMessages.containsKey(messageId)) {
			receivedBytePartLargeMessages.put(messageId, new byte[message.getNumChunks()][]);
		}
		byte[][] receivedBytePartMessages = receivedBytePartLargeMessages.get(messageId);
		receivedBytePartMessages[message.getChunkId()] = message.getBytes();

		// Received whole message.
		if (message.getChunkId() == receivedBytePartMessages.length - 1){
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
			// Send large message.
			message.getReceiver().tell(sendObject, message.getSender());
		}
		// Acknowledge receive.
		// Better as the last step.
		message.getSendingProxy().tell(new ReceivedBytesPartMessage(message.getMessageId(), message.getChunkId(), this.self()), this.self());
	}
}
