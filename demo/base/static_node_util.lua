local skynet = require "skynet"
local cluster = require "skynet.cluster"

local static_node_util = {}
local remote_service_name = {}

local noded
local self_node_name

function static_node_util.clear()
	remote_service_name = {}
end

function static_node_util.register(service_name,addr)
	cluster.register(service_name, addr)
end

function static_node_util.get_self_node_name()
	if not query_self_node_name then
		self_node_name = skynet.call(noded,"lua","query_self_node_name")
	end
	return self_node_name
end

function static_node_util.query_self_node_id()
	return skynet.call(noded,"lua","query_self_node_id")
end


function static_node_util.query_self_node_type()
	return skynet.call(noded,"lua","query_self_node_type")
end

function static_node_util.query_node_name_by_id(id)
	return skynet.call(noded,"lua","query_node_name_by_id",id)
end

function static_node_util.query_st_to_nt(source)
	return skynet.call(noded,"lua","query_st_to_nt")
end

function static_node_util.query_all_type_to_name(source)
	return skynet.call(noded,"lua","query_all_type_to_name")
end

function static_node_util.query_all_id_to_name(source)
	return skynet.call(noded,"lua","query_all_id_to_name")
end

function static_node_util.query_service_name(source)
	return skynet.call(noded,"lua","query_service_name")
end

function static_node_util.query_remote_server_name(node_name,service_name)
	local name = node_name.."."..service_name
	local remote_service = remote_service_name[name]
	if remote_service ~= nil then
		return remote_service
	end
	remote_service = cluster.query(node_name, service_name)
	remote_service_name[name] = remote_service
	return remote_service
end

function static_node_util.call(node_name,service_name,...)
	return cluster.call(node_name,service_name,...)
end

function static_node_util.send(node_name,service_name,...)
	cluster.send(node_name,service_name,...)
end

function static_node_util.send_to_node(source,des_node,des_service,...)
	local rsn = static_node_util.query_remote_server_name(des_node,des_service)
	if rsn then
		static_node_util.send(des_node,rsn,...)
		return 0
	end
	return 1
end

function static_node_util.send_to_node_by_type(des_node_type,des_service,...)
	local list= skynet.call(noded,"lua","query_node_name_by_type",des_node_type)
	if list then
		for i,v in ipairs(list) do
			local rsn = static_node_util.query_remote_server_name(v,des_service)
			print(rsn)
			if rsn then
				static_node_util.send(v,rsn,...)
			end
		end
		return 0
	else
		print("list is nil")
	end
	return 1
end

function static_node_util.query_remote_server_by_id(node_id,service_name)
	local node_name = skynet.call(noded,"lua","query_node_name_by_id",node_id)
	if node_name == nil then
		return nil
	end
	return static_node_util.query_remote_service(node_name,service_name)
end

skynet.init(function()
	noded = skynet.uniqueservice("static_noded")
end)

return static_node_util
