local skynet = require "skynet"
local skynet_manager = require "skynet.manager"

local kafka_config = require "rdkafka.config"
local kafka_producer = require "rdkafka.producer"
local topic_config = require "rdkafka.topic_config"
local topic = require "rdkafka.topic"
local pb = require "protobuf"
local JSON = require "JSON"

local topic_name = "flink-stream-test"  --"mflm-stream-topic-test"
local config
local producer
local tp_cfg
local tp

local CMD={}
local proto_list = {}

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
			producer:produce(tp,-1,payload)
		else
			print("kafka proxy encode error!")
		end
	end
end

function on_delivery(rk,err,rkt,partition,offset,payload_len)
	print("on_delivery ,partition:"..partition.." offset:"..offset.." payload_len:"..payload_len)
end

function on_stats(rk,json,json_len,opaque)
	
end

function on_error(rk,err,reason)
	print("on_error, err:"..err)
end

function on_log(r,level,fac,buf)
	print("on_log:"..buf)
end

skynet.start(function()
	pb.register_file("proto/log_proto")

	skynet.dispatch("lua", function(session, source, command, ...)	
		local f = assert(CMD[command])  --���ļ��е�CMD��û�����ݰ�!
		skynet.ret(skynet.pack(f(source, ...)))
	end)
	
	print("======kafka producer cli start===========")	
	config = kafka_config.create()
	
	config:set_delivery_cb(on_delivery)
	config:set_stat_cb(on_stats)
	config:set_error_cb(on_error)
	config:set_log_cb(on_log)

	producer = kafka_producer.create(config)
	
	skynet.fork(kafka_update,producer)	
	
	producer:brokers_add("192.168.10.89:9092,192.168.10.198:9092")
	--producer:brokers_add("192.168.10.198:9092")

	tp_cfg = topic_config.create()
	tp_cfg["auto.commit.enable"] = "true"

	tp = topic.create_for_producer(producer,topic_name,tp_cfg)


	local proto_file = io.open("./static_log","rb")
	local buffer = proto_file:read "*a"
	proto_file:close()
	pb.register(buffer)

	local test_file = io.open("./log_test.json","rb")
	buffer = test_file:read "*a"
	test_file:close()
	local t = JSON:decode(buffer)
	proto_list = t["protocol"]

	local head = {game_id=1,version=0,protoc_type=1,msg_id=4,body_size = 5,time = 6}

	skynet.sleep(200)
	do return end

	for _,v in ipairs(proto_list) do
		print(v["message_type"])
		local temp = pb.encode(v["message_type"],v["message_body"])
		head.version = head.version + 1
		head.msg_id = v.msg_id
		head.body_size = temp:len()
		head.time = skynet.time()
		local head_buf = pb.encode("statis_header",head)
		local buf = head_buf..temp
		producer:produce(tp,1,buf)
	end

end)



