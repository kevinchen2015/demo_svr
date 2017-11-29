local skynet = require "skynet"
local skynet_manager = require "skynet.manager"

local kafka_config = require "rdkafka.config"
local kafka_consumer = require "rdkafka.consumer"
local topic_config = require "rdkafka.topic_config"
local topic = require "rdkafka.topic"
local pb = require "protobuf"
local JSON = require "JSON"

local topic_name = "flink-stream-test" --"mflm-stream-topic-test" --"flink-stream-test"  
local config
local consumer
local tp_cfg
local tp

local CMD={}

local proto_list = {}
local proto_map = {}

local record = {}

--cmd: /root/tpf/kafka/kafka_2.11-0.11.0.0/bin/kafka-console-consumer.sh --zookeeper 192.168.10.53:2181/chroot/stream  --topic flink-stream-test
-- protocol     broker_id,topic_name,partition_id,payload

function kafka_update()
	while true do
		  if consumer then
			consumer:poll(5)
			consumer:consume(tp,1,1000)
    	end
			skynet.sleep(5)
	end
end

function on_stats(rk,json,json_len,opaque)
	
end

function on_error(rk,err,reason)
	print("on_error, err:"..err)
end

function on_log(r,level,fac,buf)
	print("on_log:"..buf)
end

function on_consume_msg(err,rktp,partition,payload,offset)
	--print("on_consume_msg,err:"..err)

	if err ~= 0 then
		return
	end

	if record[partition] == nil then
		record[partition] = {}
	end
	if record[partition].total == nil then
		record[partition].total = 0
	end
	if record[partition].sucess == nil then
		record[partition].sucess = 0
	end

	if #payload < 34 then
		return
	end

	record[partition].total = record[partition].total + 1

	local head_buf = string.sub(payload,1,34)
	local body_buf = string.sub(payload,-(#payload-34))

	local header = pb.decode("statis_header",head_buf)
	if header == nil then
		error("decode statis_header error!".." offset:"..offset)
		return
	end

	if header.msg_id < 1000  then
		record[partition].total = record[partition].total - 1
		print(" invaild log ,msgid:"..header.msg_id.." offset:"..offset)
		return
	end

	local proto_info = proto_map[header.msg_id]
	if proto_info == nil then
		error("header.msg_id error! id:"..header.msg_id.." offset:"..offset)
		return
	end

	local body = pb.decode(proto_info.message_type,body_buf)
	if body == nil or body == false then
		error("decode body error! id:"..header.msg_id.." offset:"..offset)
		return
	end

	local proto = proto_info["message_body"]
	local ret = comp_table(body,proto)
	if ret ~= 0 then
		print("comp body error! 1 id:"..header.msg_id.." offset:"..offset)
		return	
	end
	ret = comp_table(proto,body)
	if ret ~= 0 then
		print("comp body error! 2 id:"..header.msg_id.." offset:"..offset)
		return	
	end

	record[partition].sucess = record[partition].sucess + 1
	print("comp sucess num :"..record[partition].sucess.."/"..record[partition].total.." offset:"..offset)
end

function comp_value(l,r)
	if l==nil or r==nil then
		return 2
	end
	
	if (type(l)=="table") then
		for k,v in pairs(l) do
			--print("k:",k," v:",v," r[k]:",r[k])
			local ret = comp_value(v,r[k])
			if ret ~= 0 then
				return ret
			end
		end
	else
		--print("l:",l)
		--print(" r:",r)
		if l == r then
			return 0
		else
			return 1
		end
	end
	return 0
end

function comp_table(l,r)
	--print(" -----l:",l)
	for k,v in pairs(l) do
		if (type(v)=="table") then
			for ik,iv in pairs(v) do
				local rk = r[k]
				local ret = comp_value(iv,rk[ik])
				if ret ~= 0 then
					return ret
				end
			end
		else
			local ret = comp_value(v,r[k])
			if ret ~= 0 then
				return ret
			end
		end
	end
	return 0
end

skynet.start(function()

	skynet.dispatch("lua", function(session, source, command, ...)	
		local f = assert(CMD[command]) 
		skynet.ret(skynet.pack(f(source, ...)))
	end)
	
	print("======kafka consumer start===========")	

	local proto_file = io.open("./static_log","rb")
	local buffer = proto_file:read "*a"
	proto_file:close()
	pb.register(buffer)

	local test_file = io.open("./log_test.json","rb")
	buffer = test_file:read "*a"
	test_file:close()
	local t = JSON:decode(buffer)
	proto_list = t["protocol"]

	for _,v in ipairs(proto_list) do
		proto_map[v.msg_id] = v
	end

	config = kafka_config.create()
	config:set_stat_cb(on_stats)
	config:set_error_cb(on_error)
	config:set_log_cb(on_log)

	consumer = kafka_consumer.create(config)
	consumer:set_consume_msg_cb(on_consume_msg)
	skynet.fork(kafka_update)	
	
	consumer:brokers_add("192.168.10.89:9092,192.168.10.198:9092")

	tp_cfg = topic_config.create()
	tp = topic.create_for_consumer(consumer,topic_name,tp_cfg)

	consumer:consume_start(tp,1,kafka_consumer.RD_KAFKA_OFFSET_END) --kafka_consumer.RD_KAFKA_OFFSET_BEGINNING) kafka_consumer.RD_KAFKA_OFFSET_END

end)



