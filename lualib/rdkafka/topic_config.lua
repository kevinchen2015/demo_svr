
local librdkafka = require 'rdkafka.librdkafka'

local KafkaTopicConfig = {}
KafkaTopicConfig.__index = KafkaTopicConfig

--[[
    Create topic configuration object.
]]--

function KafkaTopicConfig.create(original_config)
    local config = {}
    setmetatable(config, KafkaTopicConfig)

    if original_config and original_config.topic_conf_ then
        rawset(config, "topic_conf_", librdkafka.rd_kafka_topic_conf_dup(original_config.topic_conf_))
    else
        rawset(config, "topic_conf_", librdkafka.rd_kafka_topic_conf_new())
    end
    return config
end

function KafkaTopicConfig:release()
    librdkafka.rd_kafka_topic_conf_destroy(self.topic_conf_)
end


--[[
    Dump the topic configuration properties and values of `conf` to a map
    with "key", "value" pairs. 
]]--
--delete!

--[[
    Sets a configuration property.

    In case of failure "error(errstr)" is called and 'errstr'
    is updated to contain a human readable error string.
]]--
function KafkaTopicConfig:__newindex(name, value)
    assert(self.topic_conf_ ~= nil)

    local ERRLEN = 256
    local ret,errbuf = librdkafka.rd_kafka_topic_conf_set(self.topic_conf_, name, value, errbuf, ERRLEN)
    if ret ~= librdkafka.RD_KAFKA_CONF_OK then
        error(errbuf)
    end
end

return KafkaTopicConfig
