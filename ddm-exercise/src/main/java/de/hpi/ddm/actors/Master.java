package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";
    private static final int REQUEST_MORE_LINES_ON_BUFFER_SIZE = 10;

    public static Props props(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
        return Props.create(Master.class, () -> new Master(reader, collector, welcomeData));
    }

    public Master(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
        this.reader = reader;
        this.collector = collector;
        this.workers = new ArrayList<>();
        this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
        this.welcomeData = welcomeData;
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    public static class StartMessage implements Serializable {
        private static final long serialVersionUID = -50374816448627600L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PasswordCrackedMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private String password;
        private int lineID;
        private ActorRef sender;
    }

    @Data
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    /////////////////
    // Actor State //
    /////////////////
    private boolean started = false;

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;
    private final ActorRef largeMessageProxy;
    private final BloomFilter welcomeData;
    private final LinkedList<ActorRef> idleWorkers = new LinkedList<>();
    private final LinkedList<String[]> lines = new LinkedList<>();
    private String passwordChars = null;
    private int passwordLength = 0;
    private boolean readerIsEmpty = false;
    private final HashMap<ActorRef, PasswordCrackingWorker.TaskCrackPasswordMessage> assignedTasks = new HashMap<>();

    private long startTime;

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
                .match(StartMessage.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .match(PasswordCrackedMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .match(RegistrationMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();
        started = true;
        this.reader.tell(new Reader.ReadMessage(), this.self());
        assignTasksToIdleWorkers();
    }

    protected void assignTasksToIdleWorkers() {
        if (!started) {
            return;
        }
        int removedWorker = 0;
        for (ActorRef idleWorker : idleWorkers) {
            String[] line;
            if ((line = lines.poll()) == null) {
                break;
            }
            removedWorker++;
            PasswordCrackingWorker.TaskCrackPasswordMessage newTask = new PasswordCrackingWorker.TaskCrackPasswordMessage(Integer.parseInt(line[0]), passwordChars, passwordLength, line[3], Arrays.copyOfRange(line, 4, line.length));
            this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(newTask, idleWorker), this.self());
            assignedTasks.put(idleWorker, newTask);
        }
        for (int i = 0; i < removedWorker; i++) {
            idleWorkers.poll();
        }
        if (!readerIsEmpty && lines.size() <= REQUEST_MORE_LINES_ON_BUFFER_SIZE) {
            this.reader.tell(new Reader.ReadMessage(), this.self());
        }
        if (readerIsEmpty && idleWorkers.size() == workers.size() && started && lines.size() == 0) {
            this.terminate();
        }
    }

    protected void handle(BatchMessage message) {

        // - The Master received the first batch of input records.
        // - To receive the next batch, we need to send another ReadMessage to the reader.
        // - If the received BatchMessage is empty, we have seen all data for this task.
        // - We need a clever protocol that forms sub-tasks from the seen records, distributes the tasks to the known workers and manages the results.
        //   -> Additional messages, maybe additional actors, code that solves the subtasks, ...
        //   -> The code in this handle function needs to be re-written.
        // - Once the entire processing is done, this.terminate() needs to be called.

        // Info: Why is the input file read in batches?
        // a) Latency hiding: The Reader is implemented such that it reads the next batch of data from disk while at the same time the requester of the current batch processes this batch.
        // b) Memory reduction: If the batches are processed sequentially, the memory consumption can be kept constant; if the entire input is read into main memory, the memory consumption scales at least linearly with the input size.
        // - It is your choice, how and if you want to make use of the batched inputs. Simply aggregate all batches in the Master and start the processing afterwards, if you wish.

        if (message.getLines().isEmpty()) {
            readerIsEmpty = true;
        }

        // Add lines to buffer.
        for (String[] line : message.getLines()) {
            if (passwordChars == null) {
                passwordChars = line[2];
                passwordLength = Integer.parseInt(line[3]);
            }
            String [] lineWithoutName = new String[line.length - 1];
            lineWithoutName[0] = line[0];
            if (line.length - 2 >= 0) System.arraycopy(line, 2, lineWithoutName, 1, line.length - 2);
            lines.add(lineWithoutName);
        }
        assignTasksToIdleWorkers();
    }

    protected void terminate() {
        this.collector.tell(new Collector.PrintMessage(), this.self());

        this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());

        for (ActorRef worker : this.workers) {
            this.context().unwatch(worker);
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }

        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());

        long executionTime = System.currentTimeMillis() - this.startTime;
        this.log().info("Algorithm finished in {} ms", executionTime);
    }

    protected void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.workers.add(this.sender());
        this.idleWorkers.add(this.sender());
        this.log().info("Registered {}", this.sender());

        this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new PasswordCrackingWorker.WelcomeMessage(this.welcomeData, this.self()), this.sender()), this.self());
        assignTasksToIdleWorkers();
    }

    protected void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        if (this.workers.contains(message.getActor())) {
            this.workers.remove(message.getActor());
            this.idleWorkers.remove(message.getActor());
            PasswordCrackingWorker.TaskCrackPasswordMessage failedTask = this.assignedTasks.remove(message.getActor());
            if (failedTask != null) {
                ArrayList<String> originalLine = new ArrayList<>();
                originalLine.add("" + failedTask.getLineID());
                originalLine.add(passwordChars);
                originalLine.add("" + passwordLength);
                originalLine.add(failedTask.getPassword());
                originalLine.addAll(Arrays.asList(failedTask.getHints()));
                this.lines.add(originalLine.toArray(new String[0]));
                this.log().info("rescheduling line: " + failedTask.getLineID());
                assignTasksToIdleWorkers();
            }
            this.log().info("Unregistered {}", message.getActor());
        }
    }

    protected void handle(PasswordCrackedMessage message) {
        this.log().info("Received cracked password: " + message.password + " line: " + message.lineID);
        this.collector.tell(new Collector.CollectMessage(message.getPassword(), message.getLineID()), this.self());
        assignedTasks.remove(message.getSender());
        idleWorkers.add(message.getSender());
        assignTasksToIdleWorkers();
    }
}
