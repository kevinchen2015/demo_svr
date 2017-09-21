local skynet = require "skynet"
local server_def = require "server_def"
local node_util = require "static_node_util"
local debug_trace = require "debug_trace"
local package_proto = require "package.proto"

local command = {}
local self_type
local st_to_nt
local server_name
local all_type_to_name
local all_id_to_name

local route_message_function = "on_route_message"
--中心路由服务,只有中心服有这个center_routed!!!
--集群内部服务之间的通信用rpc，与客户端的通信用sproto，其他外部服务可用pb or http进行通信

function command.ping(source,...)
	return ...
end

function command.route_message(source,msg,sz,uid)
	print("center_route.route_message")

	local des_type,des_id,src_type,src_id = package_proto.decode_head(msg,sz)
	print("des_type:"..des_type.." des_id:"..des_id)
	local node_type = st_to_nt[des_type]
	local service = server_name[des_type]

	if node_type == self_type then
		local local_service = skynet.uniqueservice(service.."d")   -- dirty  :(
		if local_service then
			skynet.send(local_service,"lua",route_message_function,msg,sz,uid)
		else
			print("local service can not find! des_type:"..des_type)
		end
	else
		if des_id == 0 then   --broadcast
			local node_list = all_type_to_name[node_type]
			for i,node_name in ipairs(node_list) do 
				local remote_service = node_util.query_remote_server_name(node_name,service)
				node_util.send(node_name,remote_service,route_message_function,msg,sz,uid)
			end
		else
			local node_name = all_id_to_name[des_id]
			local remote_service = node_util.query_remote_server_name(node_name,service)
			node_util.send(node_name,remote_service,route_message_function,msg,sz,uid)
		end
	end
end

skynet.start(function()

	self_type = node_util.query_self_node_type()
	st_to_nt = node_util.query_st_to_nt()
	server_name = node_util.query_service_name()
	all_type_to_name = node_util.query_all_type_to_name()
	all_id_to_name = node_util.query_all_id_to_name()
	
	skynet.dispatch("lua", function(session, source, cmd, ...)
		local f = command[cmd]
		if f then 
			skynet.ret(skynet.pack(f(source, ...)))
		else
			print("error ! command:"..cmd)
			assert(f)
			skynet.ret(skynet.pack(nil))
		end
	end)

end)
