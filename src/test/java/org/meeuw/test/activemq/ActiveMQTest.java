package org.meeuw.test.activemq;


import jakarta.jms.*;
import jakarta.jms.Queue;
import static jakarta.jms.Session.AUTO_ACKNOWLEDGE;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import lombok.extern.log4j.Log4j2;
import org.apache.activemq.ActiveMQConnectionFactory;
import static org.apache.activemq.ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE;
import org.apache.logging.log4j.Level;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.*;
import org.testcontainers.activemq.ActiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@Log4j2
public class ActiveMQTest {

    static final String[] CORRELATION_IDS = new String[] {"a", "b", "c", "d", "e"};


    @Container
    static ActiveMQContainer activemq = new ActiveMQContainer("apache/activemq-classic:6.1.6")
        .withExposedPorts(8161, 61616);

    static {
        activemq.setPortBindings(List.of("8162:8161"))

        //.withCopyFileToContainer(MountableFile.forClasspathResource("/activemq.xml"), "/opt/apache-activemq/conf/activemq.xml");
        ;
    }





    static Connection amqConnection;

    private static final Duration delay = Duration.ofSeconds(30);
    private static final int messageToSend  = 50;
    private static final int listeners  = 10;



    private static final ExecutorService executorService = Executors.newScheduledThreadPool(100);

    @BeforeAll
    public static void setUp() throws JMSException {
        ActiveMQConnectionFactory amqConnectionFactory = new ActiveMQConnectionFactory(activemq.getBrokerUrl());
        amqConnection = amqConnectionFactory.createConnection();
        amqConnection.start();
        log.info("ActiveMQ broker URL: localhost:{}", activemq.getMappedPort(8161));
    }
    @AfterAll
    public static void shutdown() throws JMSException {
        executorService.shutdown();
        amqConnection.close();
    }
    @Test
    public void testActiveMQ() throws InterruptedException {


        Instant start = Instant.now();
        Map<String, List<String>> send = new HashMap<>();
        AtomicBoolean ready = new AtomicBoolean(false);
        AtomicLong totalSent = new AtomicLong();

        executorService.submit(() -> {

                try (Session jmsSession = amqConnection.createSession(false, AUTO_ACKNOWLEDGE)) {

                    Queue destination = jmsSession.createQueue("testQueue");
                    var messageProducer = jmsSession.createProducer(destination);

                    for (int i = 0; i < messageToSend; i++) {
                        var message = message(jmsSession);
                        messageProducer.send(message);
                        totalSent.incrementAndGet();
                        log.atLevel(i % 10 == 0 ? Level.INFO : Level.DEBUG).log("{} Sent message: {}", i, message.getText());
                        send.computeIfAbsent(message.getJMSCorrelationID(), k -> new ArrayList<>())
                            .add(message.getText());

                    }
                    synchronized (ready) {
                        ready.set(true);
                        ready.notifyAll();
                    }
                    log.info("{} Messages sent to ActiveMQ", totalSent.get());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
        });

        Map<String, AtomicLong> aggregatedMessages = new HashMap<>();
        List<String> delivered = new ArrayList<>();

        AtomicLong totalReceived = new AtomicLong(0);
        AtomicInteger waiting = new AtomicInteger(0);
        DelayQueue<DelayedMessage> delayedQueue = new DelayQueue<>();

        Consumer<TextMessage> handler  = message -> {
            try {
                String correlationId = message.getJMSCorrelationID();
                long aggregated;
                synchronized (aggregatedMessages) {
                    aggregated = aggregatedMessages.remove(correlationId).get();
                    log.info("aggregatged for {}: {}", correlationId, aggregated);
                    aggregatedMessages.notifyAll();
                }
                delivered.add(((TextMessage) message).getText() + ": " + aggregated);
                message.acknowledge();
            } catch (Exception e) {
                log.error("Error processing delayed message: {}", e.getMessage(), e);
            }
        };

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Session> sessions = new ArrayList<>();
            for (int i = 0; i < listeners; i++) {
                try {
                    Session jmsSession = amqConnection.createSession(false, INDIVIDUAL_ACKNOWLEDGE);
                    sessions.add(jmsSession);
                    log.info("Starting consumer thread {}", Thread.currentThread().getName());

                    Destination destination = jmsSession.createQueue("testQueue");
                    var messageConsumer = jmsSession.createConsumer(destination);
                    messageConsumer.setMessageListener(new MessageListener(

                    ) {
                        @Override
                        public void onMessage(Message message) {
                            try {
                                synchronized (totalReceived) {
                                    totalReceived.incrementAndGet();
                                    totalReceived.notifyAll();
                                }
                                String correlationId = message.getJMSCorrelationID();
                                boolean handle;
                                synchronized (aggregatedMessages) {
                                    var l = aggregatedMessages.computeIfAbsent(correlationId, k -> new AtomicLong(0))
                                        .incrementAndGet();
                                    handle = l == 1;
                                }
                                log.info("Received message in thread {}: {}", Thread.currentThread().getName(), correlationId);
                                if (handle) {
                                    executorService.submit(() -> {
                                        try {
                                            Thread.sleep(delay.toMillis());
                                            handler.accept((TextMessage) message);
                                        } catch (Exception e) {
                                            log.error("Error aggregating message: {}", e.getMessage(), e);
                                        }
                                    });
                                } else {
                                    log.info("ackowledging message: {}", message.getJMSMessageID());
                                    message.acknowledge();
                                }
                                if (totalReceived.get() == messageToSend) {
                                    synchronized (ready) {
                                        ready.set(true);
                                        ready.notifyAll();
                                    }
                                }
                            } catch (JMSException e) {
                                log.error("Error processing message: {}", e.getMessage(), e);
                            }
                        }
                    });
                } catch (JMSException e) {
                    log.error(e.getMessage(), e);
                }
            }
            log.info("Set up listeners, waiting for messages to be sent");


            synchronized (ready) {
                while (!ready.get()) {
                    ready.wait(); // Wait for messages to be sent
                }
            }
            log.info("Everything sent, waiting for messages to be received");

            synchronized (totalReceived) {
                while (totalReceived.get() < totalSent.get()) {
                    totalReceived.wait(); // Wait for messages to be received
                }
            }

            synchronized (aggregatedMessages) {
                while(!aggregatedMessages.isEmpty()) {
                    aggregatedMessages.wait(); // Wait for all messages to be aggregated
                }
            }
            log.info("Received messages: {}", delivered);
            log.info("Sent messages: {}", send.keySet());

            assertThat(Duration.between(start, Instant.now())).isGreaterThanOrEqualTo(delay);
            sessions.forEach(s -> {
                try {
                    log.info("Closing session {}", s);
                    s.close();
                } catch (JMSException e) {
                    log.error("Error closing session: {}", e.getMessage(), e);
                }
            });
        }

    }

    Random random = new Random();
    AtomicLong counter = new AtomicLong(0);
    public TextMessage message(Session jmsSession) throws JMSException {
        var correlationId = CORRELATION_IDS[random.nextInt(CORRELATION_IDS.length)];
        var mes1 = jmsSession.createTextMessage("Hello World " + correlationId);
        mes1.setJMSCorrelationID(correlationId);
        //mes1.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay.toMillis());
        return mes1;
    }


    public record DelayedMessage(TextMessage message, Instant scheduledTime) implements Delayed {

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(Duration.between(Instant.now(), scheduledTime));
        }

        @Override
        public int compareTo(Delayed o) {
            if (o instanceof DelayedMessage other) {
                return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
            }
            return 0;
        }
    }
}

