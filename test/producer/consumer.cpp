#include "consumer.h"

SimpleAsyncConsumer:: SimpleAsyncConsumer(const std::string& brokerURI,
    const std::string& destURI,
    bool useTopic/* = false*/,
    bool clientAck/* = false*/) :
    connection(NULL),
    session(NULL),
    destination(NULL),
    consumer(NULL),
    useTopic(useTopic),
    brokerURI(brokerURI),
    destURI(destURI),
    clientAck(clientAck) {
}

SimpleAsyncConsumer:: ~SimpleAsyncConsumer()
{
    this->cleanup();
}

void SimpleAsyncConsumer:: close() {
    this->cleanup();
}

void SimpleAsyncConsumer::runConsumer()
{
    try {
        // Create a ConnectionFactory
        ActiveMQConnectionFactory* connectionFactory = new ActiveMQConnectionFactory(brokerURI);

        // Create a Connection
        connection = connectionFactory->createConnection();
        delete connectionFactory;

        ActiveMQConnection* amqConnection = dynamic_cast<ActiveMQConnection*>(connection);
        if (amqConnection != NULL) {
            amqConnection->addTransportListener(this);
        }

        connection->start();

        connection->setExceptionListener(this);

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

        // Create a MessageConsumer from the Session to the Topic or Queue
        consumer = session->createConsumer(destination);
        consumer->setMessageListener(this);

    }
    catch (CMSException& e) {
        e.printStackTrace();
    }
}

void SimpleAsyncConsumer::onMessage(const Message* message)
{
    static int count = 0;
    try {
        count++;
        const TextMessage* textMessage = dynamic_cast<const TextMessage*>(message);
        string text = "";

        if (textMessage != NULL) {
            text = textMessage->getText();
        }
        else {
            text = "NOT A TEXTMESSAGE!";
        }

        if (clientAck) {
            message->acknowledge();
        }

        printf("Message #%d Received: %s\n", count, text.c_str());
    }
    catch (CMSException& e) {
        e.printStackTrace();
    }
}

void SimpleAsyncConsumer::onException(const CMSException& ex AMQCPP_UNUSED)
{
    printf("CMS Exception occurred.  Shutting down client.\n");
    exit(1);
}

void SimpleAsyncConsumer::onException(const decaf::lang::Exception& ex)
{
    printf("Transport Exception occurred: %s \n", ex.getMessage().c_str());
}

void SimpleAsyncConsumer::transportInterrupted() 
{
    std::cout << "The Connection's Transport has been Interrupted." << std::endl;
}

void SimpleAsyncConsumer::transportResumed() 
{
    std::cout << "The Connection's Transport has been Restored." << std::endl;
}

void SimpleAsyncConsumer::cleanup()
{

    //*************************************************
    // Always close destination, consumers and producers before
    // you destroy their sessions and connection.
    //*************************************************

    // Destroy resources.
    try {
        if (destination != NULL) delete destination;
    }
    catch (CMSException& e) {}
    destination = NULL;

    try {
        if (consumer != NULL) delete consumer;
    }
    catch (CMSException& e) {}
    consumer = NULL;

    // Close open resources.
    try {
        if (session != NULL) session->close();
        if (connection != NULL) connection->close();
    }
    catch (CMSException& e) {}

    // Now Destroy them
    try {
        if (session != NULL) delete session;
    }
    catch (CMSException& e) {}
    session = NULL;

    try {
        if (connection != NULL) delete connection;
    }
    catch (CMSException& e) {}
    connection = NULL;
}