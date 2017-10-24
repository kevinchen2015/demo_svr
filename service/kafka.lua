local skynet = require "skynet"
local skynet_manager = require "skynet.manager"

local kafka_config = require "rdkafka.config"
local kafka_producer = require "rdkafka.producer"
local topic_config = require "rdkafka.topic_config"
local topic = require "rdkafka.topic"
local pb = require "protobuf"

local topic_name = "flink-stream-test"  --"mflm-stream-topic-test"
local config
local producer
local tp_cfg
local tp

local CMD={}

--cmd: /root/tpf/kafka/kafka_2.11-0.11.0.0/bin/kafka-console-consumer.sh --zookeeper 192.168.10.53:2181/chroot/stream  --topic flink-stream-test
-- protocol     broker_id,topic_name,partition_id,payload

function kafka_update(producer)
	while true do
		  if producer then
    		producer:poll(10)
    	end
			skynet.sleep(10)
	end
end

--function CMD.on_net_message(source,angent,msg,size,msg_id,conn_id)
--	if producer and tp then
--		producer:produce(tp, KAFKA_PARTITION_UA,msg)
--	end
--end

function CMD.on_net_message(source,angent,msg,size,msg_id,conn_id)
	local t = pb.decode("xgame_log_msg",msg,size)

	if t == nil then
		print("kafka proxy decode error!")
		return
	end

	if producer and tp then
		local head = {}
		head.game_id = t.game_id
		head.version = t.version
		head.protoc_type = t.protoc_type
		head.msg_type = t.msg_type
		head.size = t.body:len()
		head.time = t.time
		local payload = pb.encode("kafka_log_msg_head",head)
		if payload then
			payload = payload..t.body
			producer:produce(tp, KAFKA_PARTITION_UA,payload)
		else
			print("kafka proxy encode error!")
		end
	end
end

skynet.start(function()
	pb.register_file("proto/log_proto")

	skynet.dispatch("lua", function(session, source, command, ...)	
		local f = assert(CMD[command])  --���ļ��е�CMD��û�����ݰ�!
		skynet.ret(skynet.pack(f(source, ...)))
	end)
	
	print("======kafka cli start===========")	
	config = kafka_config.create()
	producer = kafka_producer.create(config)
	
	skynet.fork(kafka_update,producer)	
	
	producer:brokers_add("192.168.10.89:9092")
	producer:brokers_add("192.168.10.198:9092")

	tp_cfg = topic_config.create()
	tp_cfg["auto.commit.enable"] = "true"

	tp = topic.create(producer,topic_name,tp_cfg)
end)



