local skynet = require "skynet"
local redis = require "skynet.db.redis"
local pb = require "protobuf"
local debug_trace = require "debug_trace"

local config = {}
local CMD = {}
local redis_db
local requst_handle = {}
local rsp_name2id = {}


function CMD.open(source,conf)
	config = conf
	redis_db = redis.connect(config)

	--todo reconnect
end

function on_net_message(session,source,msg,size)

	local msg_struct = pb.decode("proto.db.msg_struct",msg,size)
	local fn = requst_handle[msg_struct.id]
	if fn then
		local ret,rsp_id = fn(msg_struct.body,msg_struct.body:len())
		if ret then
			local rsp = pb.encode("proto.db.msg_struct",{id = rsp_id,body = ret})
			skynet.call(source,"lua","send",rsp)
		end
	else
		print("request id is invaild! id:"..msg_struct.id)
	end
end

function string.split(str, delimiter)
	if str==nil or str=='' or delimiter==nil then
		return nil
	end
	
    local result = {}
    for match in (str..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match)
    end
    return result
end

-------------------------------------------------------------------------------
local function command_db_req_handle (msg_body,size)
	local common_db_req = pb.decode("proto.db.common_db_req",msg_body,size)

	local result = 0
	local ret = redis_db[common_db_req.cmd](redis_db,string.split(common_db_req.param," "))

	local common_db_rsp = {}
	common_db_rsp.session = common_db_req.session
	common_db_rsp.type = common_db_req.type
	common_db_rsp.client_id = common_db_req.client_id
	common_db_rsp.result = result
	common_db_rsp.content = ret

	return pb.encode("proto.db.common_db_rsp",common_db_rsp),rsp_name2id["common_db_rsp_id"]
end

-----------------------------------------------------------------------------------------

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

	rsp_name2id["common_db_rsp_id"] = pb.enum_id("proto.db.msg_id","common_db_rsp_id")
	requst_handle[pb.enum_id("proto.db.msg_id","common_db_req_id")] = command_db_req_handle
end)



