local skynet = require "skynet"
local cluster = require "skynet.cluster"
local server_def = require "server_def"
local debug_trace = require "debug_trace"

local node_config
local command = {}

local node_name = "node_name"

local node_extern_info = {}
local self_name
local node_config_path

local type_to_name = {}
local id_to_name = {}

local remote_service_name = {}

local route_server_type_to_node_type = {}
local node_service = {}

--静态节点功能,节点不变，动态节点管理采用另一套service

function command.ping(source,...)
	return ...
end

function command.query_self_node_name()
	return self_name
end

function command.query_self_node_type()
	return node_extern_info[self_name].type
end

function command.query_self_node_id()
	return node_extern_info[self_name].id
end

function command.query_node_name_by_id(source,id)
	return id_to_name[id]
end

function command.query_node_name_by_type(source,type)
	return type_to_name[type]
end

function command.query_st_to_nt(source)
	return route_server_type_to_node_type
end

function command.query_all_type_to_name(source)
	return type_to_name
end

function command.query_all_id_to_name(source)
	return id_to_name
end

function command.query_service_name(source)
	return node_service
end

function command.route_to_other_node(source,msg,sz,uid,des_type,des_id)
	--des_id 暂时不用
	local node_type = route_server_type_to_node_type[des_type]
	if node_type then
		local service_name = remote_service_name[node_type]
		local node_name = type_to_name[node_type]
		if service_name == nil then
			service_name = cluster.query(node_name, node_service[des_type])
			remote_service_name[node_type] = service_name
		end
		if service_name then
			cluster.send(node_name, service_name, "on_route_message",msg,sz,uid)
		else
			print("remote service can not finded!")
		end
	else
		print("des_type can not find route node!!!")
	end
end


function init_node_map()
	type_to_name = {}
	for k,v in pairs(node_extern_info) do
		if type_to_name[v.type] then
			table.insert(type_to_name[v.type],k)
		else
			type_to_name[v.type] = {k}
		end
	end
	--debug_trace.print_r(type_to_name)

	id_to_name = {}
	for k,v in pairs(node_extern_info) do
		id_to_name[v.id] = k
	end
end

skynet.start(function()

	node_config_path = skynet.getenv("node_config")
	assert(node_config_path)
	print("node config path:"..node_config_path)

	local config_fn = dofile(node_config_path)
	node_confg = config_fn.get_node_config()
	assert(node_confg)
	--debug_trace.print_r(node_confg)
	node_extern_info = config_fn.get_node_extern_info()
	--debug_trace.print_r(node_extern_info)
	route_server_type_to_node_type = config_fn.get_route_st_to_nt()
	--debug_trace.print_r(route_server_type_to_node_type)
	node_service = config_fn.get_node_service()
	--debug_trace.print_r(node_service)
	init_node_map()

	self_name = skynet.getenv(node_name)
	assert(self_name)
	print(self_name.." init...")

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

	--static cluster init
	cluster.reload(node_confg)
	cluster.register(self_name,skynet.self())

	cluster.open(self_name)

	print(self_name.." init succeed!")
end)
