#include <thread>
#include <chrono>
#include "kafka_client.hpp"
kafka_producer::producer my_kafka_producer;
kafka_consumer::consumer my_kafka_consumer;
void p_fun() {
    while (true) {
        for (int i = 0;i < 10;i++) {
            my_kafka_producer.produce("KAFKA", 5);
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}
void c_fun() {
    while (true) {
        std::string message;
        if (my_kafka_consumer.consume(message)) {
            std::cout << "consume message:" << message << std::endl;
        }
    }
}
int main() {
    if (!my_kafka_producer.init() || !my_kafka_consumer.init()) {
        return -1;
    }
    std::thread th0(p_fun);
    std::thread th1(c_fun);
    th0.join();
    th1.join();

    return 0;
}