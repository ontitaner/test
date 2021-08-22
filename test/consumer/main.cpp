#include "consumer.h"
#include "producer.h"

#define RUN_CONSUMER

void run_consumer()
{
    activemq::library::ActiveMQCPP::initializeLibrary();

    std::cout << "=====================================================\n";
    std::cout << "Starting the example:" << std::endl;
    std::cout << "-----------------------------------------------------\n";

    // Set the URI to point to the IPAddress of your broker.
    // add any optional params to the url to enable things like
    // tightMarshalling or tcp logging etc.  See the CMS web site for
    // a full list of configuration options.
    //
    //  http://activemq.apache.org/cms/
    //
    // Wire Format Options:
    // =====================
    // Use either stomp or openwire, the default ports are different for each
    //
    // Examples:
    //    tcp://127.0.0.1:61616                      default to openwire
    //    tcp://127.0.0.1:61616?wireFormat=openwire  same as above
    //    tcp://127.0.0.1:61613?wireFormat=stomp     use stomp instead
    //
    std::string brokerURI =
        "failover:(tcp://127.0.0.1:61616"
        //        "?wireFormat=openwire"
        //        "&connection.useAsyncSend=true"
        //        "&transport.commandTracingEnabled=true"
        //        "&transport.tcpTracingEnabled=true"
        //        "&wireFormat.tightEncodingEnabled=true"
        ")";

    //============================================================
    // This is the Destination Name and URI options.  Use this to
    // customize where the consumer listens, to have the consumer
    // use a topic or queue set the 'useTopics' flag.
    //============================================================
    std::string destURI = "TEST.FOO"; //?consumer.prefetchSize=1";

    //============================================================
    // set to true to use topics instead of queues
    // Note in the code above that this causes createTopic or
    // createQueue to be used in the consumer.
    //============================================================
    bool useTopics = true;

    //============================================================
    // set to true if you want the consumer to use client ack mode
    // instead of the default auto ack mode.
    //============================================================
    bool clientAck = false;

    // Create the consumer
    SimpleAsyncConsumer consumer(brokerURI, destURI, useTopics, clientAck);

    // Start it up and it will listen forever.
    consumer.runConsumer();

    // Wait to exit.
    std::cout << "Press 'q' to quit" << std::endl;
    while (std::cin.get() != 'q') {}

    // All CMS resources should be closed before the library is shutdown.
    consumer.close();

    std::cout << "-----------------------------------------------------\n";
    std::cout << "Finished with the example." << std::endl;
    std::cout << "=====================================================\n";

    activemq::library::ActiveMQCPP::shutdownLibrary();
}

void run_producer()
{
    activemq::library::ActiveMQCPP::initializeLibrary();

    std::cout << "=====================================================\n";
    std::cout << "Starting the example:" << std::endl;
    std::cout << "-----------------------------------------------------\n";

    // Set the URI to point to the IPAddress of your broker.
    // add any optional params to the url to enable things like
    // tightMarshalling or tcp logging etc.  See the CMS web site for
    // a full list of configuration options.
    //
    //  http://activemq.apache.org/cms/
    //
    // Wire Format Options:
    // =====================
    // Use either stomp or openwire, the default ports are different for each
    //
    // Examples:
    //    tcp://127.0.0.1:61616                      default to openwire
    //    tcp://127.0.0.1:61616?wireFormat=openwire  same as above
    //    tcp://127.0.0.1:61613?wireFormat=stomp     use stomp instead
    //
    std::string brokerURI =
        "failover://(tcp://127.0.0.1:61616"
        //        "?wireFormat=openwire"
        //        "&connection.useAsyncSend=true"
        //        "&transport.commandTracingEnabled=true"
        //        "&transport.tcpTracingEnabled=true"
        //        "&wireFormat.tightEncodingEnabled=true"
        ")";

    //============================================================
    // Total number of messages for this producer to send.
    //============================================================
    unsigned int numMessages = 2000;

    //============================================================
    // This is the Destination Name and URI options.  Use this to
    // customize where the Producer produces, to have the producer
    // use a topic or queue set the 'useTopics' flag.
    //============================================================
    std::string destURI = "TEST.FOO";

    //============================================================
    // set to true to use topics instead of queues
    // Note in the code above that this causes createTopic or
    // createQueue to be used in the producer.
    //============================================================
    bool useTopics = false;

    // Create the producer and run it.
    SimpleProducer producer(brokerURI, numMessages, destURI, useTopics);

    // Publish the given number of Messages
    producer.run();

    // Before exiting we ensure that all CMS resources are closed.
    producer.close();

    std::cout << "-----------------------------------------------------\n";
    std::cout << "Finished with the example." << std::endl;
    std::cout << "=====================================================\n";

    activemq::library::ActiveMQCPP::shutdownLibrary();
}

int main(int argc, char* argv[])
{
#ifdef RUN_CONSUMER
    run_consumer();
#else
    run_producer();
#endif // RUN_CONSUMER

	return 0;
}