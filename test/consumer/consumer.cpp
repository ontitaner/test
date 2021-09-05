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
    string text = "";
    int     cnt;
    unsigned char uc;
    char ch;
    vector<unsigned char>vec_bytes;

    try {
        count++;
        const MapMessage* mapMessage = dynamic_cast<const MapMessage*>(message);

        if (mapMessage->isEmpty())
        {
            printf("message is empty\n");
            return;
        }

        /*enum ValueType {
            NULL_TYPE = 0,
            BOOLEAN_TYPE = 1,
            BYTE_TYPE = 2,
            CHAR_TYPE = 3,
            SHORT_TYPE = 4,
            INTEGER_TYPE = 5,
            LONG_TYPE = 6,
            DOUBLE_TYPE = 7,
            FLOAT_TYPE = 8,
            STRING_TYPE = 9,
            BYTE_ARRAY_TYPE = 10,
            UNKNOWN_TYPE = 11
        };*/

        auto vec = mapMessage->getMapNames();
        for (auto iter : vec)
        {
            Message::ValueType value_type = mapMessage->getValueType(iter);
            switch (value_type)
            {
            case 2:
                uc = mapMessage->getByte(iter);
                break;
            case 3:
                ch = mapMessage->getChar(iter);
                break;
            case 5:
                cnt = mapMessage->getInt(iter);
                break;
            case 9:
                text = mapMessage->getString(iter);
                break;
            case 10:
                vec_bytes = mapMessage->getBytes(iter);
                break;
            default:
                break;
            }
        }
        printf("Message #%d Received string:%s char:%c byte:%02x int:%d ", \
            count, text.c_str(), ch, uc, cnt);

        for (auto iter1 : vec_bytes)
        {
            printf("%02x,", iter1);
        }
        printf("\n");

        if (clientAck) {
            message->acknowledge();
        }

        
    }
    catch (CMSException& e) {
        e.printStackTrace();
    }
    Sleep(1000);
}

void SimpleAsyncConsumer::onException(const CMSException& ex AMQCPP_UNUSED)
{
    printf("CMS Exception occurred.  Shutting down client.\n");
    exit(2);
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