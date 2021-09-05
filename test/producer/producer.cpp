#include "producer.h"
//#include <sstream>

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
        int cnt             { 0 };
        char ch             { 'A' };
        string threadIdStr  { Long::toString(Thread::currentThread()->getId()) };
        vector<unsigned char> vec{0x01,0x02,0x03};
        unsigned char uc = 0x05;

        // Create a messages
        //string text = (string)"Hello world! from thread " + threadIdStr;
        while (1)
        {
            string text = "hello world" + to_string(++cnt);

            MapMessage* message = session->createMapMessage();
            message->setString("producer", text);
            message->setInt("int", cnt);
            message->setByte("set_byte", uc);
            message->setBytes("set_bytes", vec);
            message->setChar("set_char",ch);

            producer->send(message);
            delete message;
            Sleep(1500);



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
