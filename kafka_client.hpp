#pragma once
#include <time.h>
#include <stdint.h>
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include "rdkafkacpp.h"
namespace kafka_producer {
static const int STAT_INTERVAL = 60;
class delivery_report_cb : public RdKafka::DeliveryReportCb {
public:
    delivery_report_cb() = default;
    virtual ~delivery_report_cb() = default;
public:
    void dr_cb(RdKafka::Message &message) {
        if (message.err() != RdKafka::ERR_NO_ERROR) {
            std::cerr << "dr_cb error:" << message.errstr() << std::endl;
            return;
        }
        send_msg_num_++;
        send_msg_size_ += message.len();
        if (time(nullptr) - send_time_ >= STAT_INTERVAL) {
            std::cout << "produce:" << send_msg_num_ << " messages/min " << send_msg_size_ << " bytes/min" << std::endl;
            send_msg_num_ = 0;    
            send_msg_size_ = 0;
            send_time_ = time(nullptr);
        }
    }
private:
    int send_msg_num_ = 0;
    time_t send_time_ = time(nullptr);
    uint64_t send_msg_size_ = 0;    
};
class hash_partitioner_cb : public RdKafka::PartitionerCb {
public:
    int32_t partitioner_cb(const RdKafka::Topic *topic, 
                           const std::string *key, 
                           int32_t partition_cnt, 
                           void *msg_opaque) {
        return hash_fun(key->c_str(), key->size()) % partition_cnt;
    }
private:
    static unsigned int hash_fun(const char *str, size_t len) {
        unsigned int hash = 5381;
        for (size_t i = 0;i < len;i++) {
            hash  = ((hash << 5) + hash) + str[i];
        }
        return hash;
    }
};
class producer {
public:
    producer() {
        load_config();
    }
    ~producer() {
        RdKafka::wait_destroyed(WAIT_TIME_OUT);
    }
public:
    bool init() {
        if (!create_conf()) {
            return false;
        }
        if (!set_conf() || !set_tconf()) {
            return false;
        }
        set_ready();
        if (!connect()) {
            return false;
        }
        set_enabled();
        return true;
    }
    void load_config() {
        // conf
        conf_config_map_["metadata.broker.list"] = "127.0.0.1:9092";
        conf_config_map_["compression.codec"] = "";
        conf_config_map_["broker.version.fallback"] = "";
        conf_config_map_["sasl.username"] = "kafka";
        conf_config_map_["sasl.password"] = "kafka#secret";
        conf_config_map_["api.version.request"] = "true";
        conf_config_map_["security.protocol"] = "sasl_plaintext";
        conf_config_map_["sasl.mechanisms"] = "PLAIN";
        conf_config_map_["queue.buffering.max.messages"] = "1000000";
        conf_config_map_["queue.buffering.max.kbytes"] = "";
        conf_config_map_["queue.buffering.max.ms"] = "100";
        conf_config_map_["batch.num.messages"] = "10000";
        conf_config_map_["socket.keepalive.enable"] = "true";
        conf_config_map_["message.send.max.retries"] = "3";
        // tconf
        tconf_config_map_["request.required.acks"] = "1";
    }
    void load_config(const std::map<std::string, std::string>conf_config_map, 
                     const std::map<std::string, std::string>tconf_config_map) {
        for (auto &e : conf_config_map) {
            conf_config_map_[e.first] = e.second;
        }
        for (auto &e : tconf_config_map) {
            tconf_config_map_[e.first] = e.second;
        }
    }
    inline bool create_conf() {
        conf_ = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        tconf_ = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
        if (!conf_ || !tconf_) {
            return false;
        }
        return true;
    }
    bool set_conf() {
        RdKafka::Conf::ConfResult r = RdKafka::Conf::CONF_UNKNOWN;
        for (auto &iterm : conf_config_map_) {
            if (iterm.second.empty()) {
                continue;
            }
            r = conf_->set(iterm.first, iterm.second, errstr_);
            if (r != RdKafka::Conf::CONF_OK) {
                std::cerr << "set conf error:" << errstr_ << std::endl;
                return false;
            }
        }
        if (conf_->set("dr_cb", &dr_cb_, errstr_) != RdKafka::Conf::CONF_OK) {
            std::cerr << "set conf error:" << errstr_ << std::endl;
            return false;
        }    
        return true;
    }
    bool set_tconf() {
        RdKafka::Conf::ConfResult r = RdKafka::Conf::CONF_UNKNOWN;
        for (auto &iterm : tconf_config_map_) {
            if (iterm.second.empty()) {
                continue;
            }
            r = tconf_->set(iterm.first, iterm.second, errstr_);
            if (r != RdKafka::Conf::CONF_OK) {
                std::cerr << "set conf error:" << errstr_ << std::endl;
                return false;
            }
        }
        if (tconf_->set("partitioner_cb", &hash_partitioner_cb_, errstr_) != RdKafka::Conf::CONF_OK) {
            std::cerr << "set tconf error:" << errstr_ << std::endl;
            return false;
        }    
        return true;
    }
    inline void set_topic(const std::string &str) {
        topic_str_ = str;
    }
    inline bool is_enabled() const {
        return is_enabled_;
    }
    inline void set_enabled(bool flag = true) {
        is_enabled_ = flag;
    }
    inline void set_ready(bool flag = true) {
        is_ready_ = flag;
    }
    bool connect() {
        if (!is_ready_) {
            return false;
        }
        if (topic_str_.empty()) {
            return false;
        }
        producer_ = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf_.get(), errstr_));
        if (!producer_) {
            std::cerr << "kafka producer connect failed error:" << errstr_ << std::endl;
            return false;
        }
        topic_ = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(producer_.get(), topic_str_, tconf_.get(), errstr_));
        if (!topic_) {
            std::cerr << "kafka producer connect failed error:" << errstr_ << std::endl;
            return false;
        }
        is_connected_ = true;
        return true;
    }
    bool produce(const char *str, size_t size) {
        if (!is_enabled_) {
            return false;
        }
        if (!is_connected_) {
            destroyed();
            if (!connect()) {
                return false;
            }
        }
        bool succ = true;
        RdKafka::ErrorCode ret = producer_->produce(topic_.get(),
                                                     partition_,
                                                     RdKafka::Producer::RK_MSG_COPY, 
                                                     const_cast<char *>(str), 
                                                     size,
                                                     nullptr,
                                                     nullptr);
        switch (ret) {
        case RdKafka::ERR_NO_ERROR:
            if (drop_message_cnt_ > 0 && (time(nullptr) - drop_message_timestamp_ >= STAT_INTERVAL)) {
                std::cerr << "librdkafka queue is full drop messages count:" << drop_message_cnt_ << std::endl;
                drop_message_timestamp_ = time(nullptr);
                drop_message_cnt_ = 0;
            }
            break;
        case RdKafka::ERR__QUEUE_FULL:
            ++drop_message_cnt_;
            if (time(nullptr) - drop_message_timestamp_ >= STAT_INTERVAL) {
                std::cerr << "librdkafka queue is full drop messages count:" << drop_message_cnt_ << std::endl;
                drop_message_timestamp_ = time(nullptr);
                drop_message_cnt_ = 0;
            }
            succ = false;
            break;
        default:
            is_connected_ = false;
            succ = false;
            std::cerr << "kafka message produce failed error:" << RdKafka::err2str(ret) << std::endl;
            break;
        }
        producer_->poll(0);
        return succ;
    }
private:
    void destroyed() {
        topic_.reset();
        producer_.reset();
    }
private:
    delivery_report_cb dr_cb_;
    hash_partitioner_cb hash_partitioner_cb_;
private:
    std::unique_ptr<RdKafka::Conf>conf_;
    std::unique_ptr<RdKafka::Conf>tconf_;
    std::unique_ptr<RdKafka::Producer>producer_;
    std::unique_ptr<RdKafka::Topic>topic_;
private:
    int partition_ = RdKafka::Topic::PARTITION_UA;
private:
    time_t drop_message_timestamp_ = time(nullptr);
    uint64_t drop_message_cnt_ = 0;
private:
    bool is_connected_ = false;
    bool is_ready_ = false;
    bool is_enabled_ = false;
private:
    std::map<std::string, std::string>conf_config_map_;   // key -- config item value -- config item value
    std::map<std::string, std::string>tconf_config_map_;  // key -- config item value -- config item value
    std::string topic_str_ = "icsCenter";
    std::string errstr_;
private:
    const int WAIT_TIME_OUT = 50;
};
}
namespace kafka_consumer {
static const int STAT_INTERVAL = 60;
class consumer_rebalance_cb : public RdKafka::RebalanceCb {
private:
	void rebalance_cb(RdKafka::KafkaConsumer *consumer,
                      RdKafka::ErrorCode ret, 
                      std::vector<RdKafka::TopicPartition *>&partition) {
        if (RdKafka::ERR__ASSIGN_PARTITIONS == ret) {
            consumer->assign(partition);
            return;
        }
        consumer->unassign();
    }
};
class consumer {
public:
	consumer() {
        load_config();
    }
    ~consumer() {
        destroyed();
        RdKafka::wait_destroyed(WAIT_TIME_OUT);
    }
public:
	bool init() {
        if (!create_conf()) {
            return false;
        }
        if (!set_conf()) {
            return false;
        }
        set_ready();
        if (!connect()) {
            return false;
        }
        set_enabled();
        return true;
    }
    inline void set_topics(const std::vector<std::string>&vec) {
        topics_.clear();
        for (auto &topic : vec) {
            topics_.emplace_back(topic);
        }
    }
    void load_config() {
        // conf
        conf_config_map_["metadata.broker.list"] = "127.0.0.1:9092";
        conf_config_map_["group.id"] = "test";
        conf_config_map_["sasl.username"] = "kafka";
        conf_config_map_["sasl.password"] = "kafka#secret";
        conf_config_map_["api.version.request"] = "true";
        conf_config_map_["security.protocol"] = "sasl_plaintext";
        conf_config_map_["sasl.mechanisms"] = "PLAIN";
        conf_config_map_["socket.keepalive.enable"] = "true";
        conf_config_map_["auto.offset.reset"] = "latest";
    }
    void load_config(const std::map<std::string, std::string>conf_config_map) {
        for (auto &e : conf_config_map) {
            conf_config_map_[e.first] = e.second;
        }
    }

    inline bool create_conf() {
        conf_ = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        if (!conf_) {
            return false;
        }
        return true;
    }
    bool set_conf() {
        RdKafka::Conf::ConfResult r = RdKafka::Conf::CONF_UNKNOWN;
        for (auto &iterm : conf_config_map_) {
            if (iterm.second.empty()) {
                continue;
            }
            r = conf_->set(iterm.first, iterm.second, errstr_);
            if (r != RdKafka::Conf::CONF_OK) {
                std::cerr << "set conf error:" << errstr_ << std::endl;
                return false;
            }
        }
        if (conf_->set("rebalance_cb", &rebalance_cb_, errstr_) != RdKafka::Conf::CONF_OK) {
            std::cerr << "set conf error:" << errstr_ << std::endl;
            return false;
        }    
        return true;
    }
    inline bool create_consumer() {
        consumer_ = std::unique_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(conf_.get(), errstr_));
        if (!consumer_) {
            std::cerr << "create consumer error:" << errstr_ << std::endl;
            return false;
        }
        RdKafka::ErrorCode ret = consumer_->subscribe(topics_);
        if (RdKafka::ERR_NO_ERROR != ret) {
            std::cerr << "subcribe topic error:" << RdKafka::err2str(ret) << std::endl;
            return false;
        }
	    return true;
    }
    inline bool is_enabled() const {
        return is_enabled_;
    }
    inline void set_enabled(bool flag = true) {
        is_enabled_ = flag;
    }
    inline void set_ready(bool flag = true) {
        is_ready_ = flag;
    }
    bool connect() {
        if (!is_ready_) {
            return false;
        }
        if (topics_.empty()) {
            return false;
        }
        if (false == create_consumer()) {
            return false;
        }
        is_connected_ = true;
        return true;
    }
	bool consume(std::string &message) {
        if (!is_enabled_) {
            return false;
        }
        if (!is_connected_) {
            destroyed();
            if (!connect()) {
                return false;
            }
        }
        char *start = nullptr;
        size_t len = 0;
        std::unique_ptr<RdKafka::Message>msg(consumer_->consume(WAIT_TIME_OUT));
        if (nullptr == msg) {
            return false;
        }
        bool succ = false;
        RdKafka::ErrorCode ret = msg->err();
        switch (ret) {
        case RdKafka::ERR_NO_ERROR:
            start = static_cast<char *>(msg->payload());
            len = msg->len();
            if (nullptr != start && len > 0) {
                message.assign(start, len);
                succ = true;
            }
            consume_msg_num_++;
            consume_msg_size_ += len;
            if (time(nullptr) - consume_time_ >= STAT_INTERVAL) {
                std::cout << "consume:" << consume_msg_num_ << " messages/min " << consume_msg_size_ << " bytes/min" << std::endl;
                consume_msg_num_ = 0;    
                consume_msg_size_ = 0;
                consume_time_ = time(nullptr);
            }
            break;
        case RdKafka::ERR__TIMED_OUT:
            break;
        case RdKafka::ERR__PARTITION_EOF:   // Broker:no more message
            break;
        default:
            is_connected_ = false;
            std::cerr << "kafka message consume failed error:" << RdKafka::err2str(ret) << std::endl;
            break;
        }
        return succ;
    }
private:
    void destroyed() {
        if (consumer_) {
            consumer_->close();
            consumer_.reset();
        }
    }
public:
    std::vector<std::string>topics_{ "icsCenter" };
    std::string errstr_;
private:
    consumer_rebalance_cb rebalance_cb_;
private:
	std::unique_ptr<RdKafka::Conf>conf_;
	std::unique_ptr<RdKafka::KafkaConsumer>consumer_;
private:
   std::map<std::string, std::string>conf_config_map_;   // key -- config item value -- config item value
private:
    bool is_connected_ = false;
    bool is_ready_ = false;
    bool is_enabled_ = false;
private:
    int consume_msg_num_ = 0;       
    time_t consume_time_ = time(nullptr);
    uint64_t consume_msg_size_ = 0;    
private:
    const int WAIT_TIME_OUT = 1000;  // 1000ms
};
}