
local librdkafka = require 'rdkafka.librdkafka'
local KafkaConfig = require 'rdkafka.config'
local KafkaTopic = require 'rdkafka.topic'

local DEFAULT_DESTROY_TIMEOUT_MS = 3000

local KafkaConsumer = {
    RD_KAFKA_OFFSET_BEGINNING = librdkafka.RD_KAFKA_OFFSET_BEGINNING,
    RD_KAFKA_OFFSET_END = librdkafka.RD_KAFKA_OFFSET_END,
    RD_KAFKA_OFFSET_STORED = librdkafka.RD_KAFKA_OFFSET_STORED
}

KafkaConsumer.__index = KafkaConsumer

function KafkaConsumer.create(kafka_config, destroy_timeout_ms)
    local config = kafka_config.kafka_conf_
    --if kafka_config ~= nil then
    --    config = KafkaConfig.create(kafka_config).kafka_conf_
    --end
    local ERRLEN = 256
    local kafka,errbuf = librdkafka.rd_kafka_new(librdkafka.RD_KAFKA_CONSUMER, config, errbuf, ERRLEN)

    if kafka == nil then
        error(errbuf)
    end

    local consumer = {kafka_ = kafka,destory_timeout_ms_ = destroy_timeout_ms}
    KafkaTopic.kafka_topic_map_[kafka] = {}
    setmetatable(consumer, KafkaConsumer)
    return consumer
end

function KafkaConsumer:release()

    while librdkafka.rd_kafka_outq_len(self.kafka_) > 0 do
        librdkafka.rd_kafka_poll(self.kafka_, 10);
    end

    for k, topic_ in pairs(KafkaTopic.kafka_topic_map_[self.kafka_]) do
        librdkafka.rd_kafka_topic_destroy(topic_) 
    end
    KafkaTopic.kafka_topic_map_[self.kafka_] = nil
    librdkafka.rd_kafka_destroy(self.kafka_)
    librdkafka.rd_kafka_wait_destroyed(self.destory_timeout_ms_ or DEFAULT_DESTROY_TIMEOUT_MS)    
end


function KafkaConsumer:brokers_add(broker_list)
    assert(self.kafka_ ~= nil)
    return librdkafka.rd_kafka_brokers_add(self.kafka_, broker_list)
end


function KafkaConsumer:set_consume_msg_cb(func)
    self.msg_cb = func
end

function KafkaConsumer:consume_start(topic,partition,start_offset)
    return librdkafka.rd_kafka_consume_start(topic.topic_,partition,start_offset)
end

function KafkaConsumer:consume_stop(topic,partition)
    librdkafka.rd_kafka_consume_stop(topic.topic_,partition)
end

function KafkaConsumer:consume(topic,partition,timeout)
    librdkafka.rd_kafka_consume(topic.topic_,partition,timeout,self.msg_cb)
end

function KafkaConsumer:poll(timeout_ms)
    assert(self.kafka_ ~= nil)
    return librdkafka.rd_kafka_poll(self.kafka_, timeout_ms)
end

return KafkaConsumer
