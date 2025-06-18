package org.meeuw.test.activemq;


import jakarta.jms.*;
import static jakarta.jms.Session.AUTO_ACKNOWLEDGE;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import lombok.extern.log4j.Log4j2;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.activemq.ActiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
@Log4j2
public class ActiveMQTest {


    @Container
    static ActiveMQContainer activemq = new ActiveMQContainer("apache/activemq-classic:5.18.3")
        .withCopyFileToContainer(MountableFile.forClasspathResource("/activemq.xml"), "/opt/apache-activemq/conf/activemq.xml");





    static Connection amqConnection;

    private Duration delay = Duration.ofSeconds(10);
    @BeforeAll
    public static void setUp() throws JMSException {
        ActiveMQConnectionFactory amqConnectionFactory = new ActiveMQConnectionFactory(activemq.getBrokerUrl());
        amqConnection = amqConnectionFactory.createConnection();
        amqConnection.start();
        log.info("ActiveMQ broker URL: localhost:{}", activemq.getMappedPort(8161));
    }
    @Test
    public void testActiveMQ() throws JMSException, InterruptedException {

        Instant start = Instant.now();
        try (Session jmsSession = amqConnection.createSession(false, AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsSession.createQueue("testQueue");
            var messageProducer = jmsSession.createProducer(destination);

            var mes1 = jmsSession.createTextMessage("Hello World");
            mes1.setJMSCorrelationID("a");
            mes1.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay.toMillis());

            var mes2 = jmsSession.createTextMessage("Hello World");
            mes2.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay.toMillis());
            mes2.setJMSCorrelationID("a");
            messageProducer.send(mes1);
            messageProducer.send(mes2);

        }
        log.info("Messages sent to ActiveMQ");

        Map<String, List<String>> aggregatedMessages = new HashMap<>();

        try (Session jmsSession = amqConnection.createSession(false, AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsSession.createQueue("testQueue");
            var messageConsumer = jmsSession.createConsumer(destination);
            messageConsumer.setMessageListener(message -> {
                try {
                    String correlationID = message.getJMSCorrelationID();
                    String text = ((TextMessage) message).getText();
                    synchronized (aggregatedMessages) {
                        log.info("Received message: {}", ((TextMessage) message).getText());
                        aggregatedMessages
                            .computeIfAbsent(correlationID, k -> new ArrayList<>())
                          .add(text);
                        aggregatedMessages.notifyAll();
                    }
                } catch (JMSException e) {
                    log.error("Error processing message", e);
                }
            });



            synchronized (aggregatedMessages) {
                while( (!aggregatedMessages.containsKey("a")) || aggregatedMessages.get("a").size() < 2) {

                    aggregatedMessages.wait(); // Wait for messages to be received
                }
            }
            log.info("Received messages: {}", aggregatedMessages.get("a"));
            assertThat(Duration.between(start, Instant.now())).isGreaterThanOrEqualTo(delay);
        }
    }

}
