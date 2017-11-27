local skynet = require "skynet"
local redis = require "skynet.db.redis"
local pb = require "protobuf"
local debug_trace = require "debug_trace"
local netpack = require "skynet.netpack"  

local CMD = {}

local requst_handle = {}
local rsp_name2id = {}

--redis 请求的协议编解码和请求转发，可根据type转发，根据client_id hash均摊

local worker_group = {}   --key:type,  value: worker list
local worker_config = {}  --key:type,  value: redis cli config

-------------------------------------------------------------------------------------------------------

function on_net_message(session,source,msg,size)
	local msg_struct = pb.decode("proto.db.msg_struct",msg,size)
	local m = netpack.tostring(msg,size)
	local fn = requst_handle[msg_struct.id]
	if fn then
		local ret,rsp_id = fn(msg_struct.body,msg_struct.body:len())
		if ret then
			local rsp = pb.encode("proto.db.msg_struct",{id = rsp_id,body = ret})
			skynet.send(source,"lua","send",rsp)
		end
	else
		print("request id is invaild! id:"..msg_struct.id)
	end
end

-------------------------------------------------------------------------------
local function command_db_req_handle (msg_body,size)
	local common_db_req = pb.decode("proto.db.common_db_req",msg_body,size)
	
	local result = 0
	local worker = get_woker(common_db_req.type,common_db_req.client_id)
	local ret = "worker not find"
	if worker ~= nil then
		ret = skynet.call(worker,"lua","execute",common_db_req.cmd,common_db_req.param,skynet.time())
	end	
	local common_db_rsp = {}
	common_db_rsp.session = common_db_req.session
	common_db_rsp.type = common_db_req.type
	common_db_rsp.client_id = common_db_req.client_id
	common_db_rsp.result = result
	common_db_rsp.content = ret

	return pb.encode("proto.db.common_db_rsp",common_db_rsp),rsp_name2id["common_db_rsp_id"]
end

-----------------------------------------------------------------------------------------

function get_woker(type,client_id)
	local worker_list = worker_group[type]
	if worker_list == nil then return nil end

	if #worker_list > 1 then
		local index = math.fmod(client_id,#worker_list)
		if index == 0 then index = 1 end
		return worker_list[index]
	else
		return worker_list[1]
	end
end

function worker_list_init(config)
	local worker_list = {}
	for i,v in ipairs(config.port_list) do
		worker = skynet.newservice("redis_cli_worker")
		skynet.send(worker,"lua","open",{host = config.host,port = v,db = config.db})
		table.insert(worker_list,worker)
	end 
	return worker_list
end

function worker_init()
	local conf = {
		host = "192.168.10.53",
		port_list = {19000},
		db = 0,
	}
	local worker_list = worker_list_init(conf)

	worker_group[1] = worker_list
	worker_config[1] = conf

	--todo
	worker_group[2] = worker_list
	worker_config[2] = conf

	worker_group[3] = worker_list
	worker_config[3] = conf
end

------------------------------------------------------------------------------------

skynet.register_protocol {
	name = "client",
	id = skynet.PTYPE_CLIENT,
	unpack = function(msg,size)
		return msg,size
	end,
	dispatch = on_net_message
}

skynet.start(function()
	pb.register_file("proto/db_proto")

	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)

	--proto from pb
	rsp_name2id["common_db_rsp_id"] = pb.enum_id("proto.db.msg_id","common_db_rsp_id")
	requst_handle[pb.enum_id("proto.db.msg_id","common_db_req_id")] = command_db_req_handle

	--create worker group
	worker_init()
end)



