#include "producer.h"

SimpleProducer::SimpleProducer(const std::string& brokerURI, unsigned int numMessages,
    const std::string& destURI, bool useTopic/* = false*/, bool clientAck/* = false*/) :
    connection(NULL),
    session(NULL),
    destination(NULL),
    producer(NULL),
    useTopic(useTopic),
    clientAck(clientAck),
    numMessages(numMessages),
    brokerURI(brokerURI),
    destURI(destURI) {
}

SimpleProducer:: ~SimpleProducer() {
    cleanup();
}

void SimpleProducer::close() {
    this->cleanup();
}

void SimpleProducer::run()
{
    try {

        // Create a ConnectionFactory
        auto_ptr<ActiveMQConnectionFactory> connectionFactory(
            new ActiveMQConnectionFactory(brokerURI));

        // Create a Connection
        try {
            connection = connectionFactory->createConnection();
            connection->start();
        }
        catch (CMSException& e) {
            e.printStackTrace();
            throw e;
        }

        // Create a Session
        if (clientAck) {
            session = connection->createSession(Session::CLIENT_ACKNOWLEDGE);
        }
        else {
            session = connection->createSession(Session::AUTO_ACKNOWLEDGE);
        }

        // Create the destination (Topic or Queue)
        if (useTopic) {
            destination = session->createTopic(destURI);
        }
        else {
            destination = session->createQueue(destURI);
        }

        // Create a MessageProducer from the Session to the Topic or Queue
        producer = session->createProducer(destination);
        producer->setDeliveryMode(DeliveryMode::NON_PERSISTENT);

        // Create the Thread Id String
        string threadIdStr = Long::toString(Thread::currentThread()->getId());

        // Create a messages
        string text = (string)"Hello world! from thread " + threadIdStr;

        for (unsigned int ix = 0; ix < numMessages; ++ix) {
            TextMessage* message = session->createTextMessage(text);

            message->setIntProperty("Integer", ix);

            // Tell the producer to send the message
            printf("Sent message #%d from thread %s\n", ix + 1, threadIdStr.c_str());
            producer->send(message);

            delete message;
        }

    }
    catch (CMSException& e) {
        e.printStackTrace();
    }
}

void SimpleProducer::cleanup() {

    // Destroy resources.
    try {
        if (destination != NULL) delete destination;
    }
    catch (CMSException& e) { e.printStackTrace(); }
    destination = NULL;

    try {
        if (producer != NULL) delete producer;
    }
    catch (CMSException& e) { e.printStackTrace(); }
    producer = NULL;

    // Close open resources.
    try {
        if (session != NULL) session->close();
        if (connection != NULL) connection->close();
    }
    catch (CMSException& e) { e.printStackTrace(); }

    try {
        if (session != NULL) delete session;
    }
    catch (CMSException& e) { e.printStackTrace(); }
    session = NULL;

    try {
        if (connection != NULL) delete connection;
    }
    catch (CMSException& e) { e.printStackTrace(); }
    connection = NULL;
}
