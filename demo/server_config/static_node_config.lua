local server_def = require "server_def"

--整个集群的配置信息

local config_function = {}

--节点ip端口配置
local node_config = {
	--单例服
	center_node = "127.0.0.1:10001",
	--match_node = "127.0.0.1:30001",
	social_node = "127.0.0.1:50001",
	
	--可平行扩展服
	gate_1_node = "127.0.0.1:20001",
	roomgroup_1_node = "127.0.0.1:40001",
	roomgroup_2_node = "127.0.0.1:40002",
}



--节点附加信息 type 与 node_type 一致，id全局唯一,0为无效
local node_extern_info = {
	center_node = {
		type = server_def.node_type.center_node,
		id = 1,
	},
	
	gate_1_node = {
		type = server_def.node_type.gate_node,
		id = 1001,
	},
	
	--match_node = {
	--	type = server_def.node_type.match_node,
	--	id = 2002,
	--},
	
	social_node = {
		type = server_def.node_type.social_node,
		id = 4003,
	},
	
	roomgroup_1_node = {
		type = server_def.node_type.room_group_node,
		id = 3001,
	},
	
	roomgroup_2_node = {
		type = server_def.node_type.room_group_node,
		id = 3002,
	},
}

--node route 用到的路由关系
local route_server_type_to_node_type = {
	[server_def.service_type.gate] = server_def.node_type.gate_node,
	[server_def.service_type.match] = server_def.node_type.center_node,
	--[server_def.service_type.room] = server_def.node_type.match_node,
	[server_def.service_type.pvp] = server_def.node_type.room_group_node,
	[server_def.service_type.social] = server_def.node_type.social_node,
	[server_def.service_type.chat] = server_def.node_type.social_node,
}

--node 中的服务命名
local node_service = {
	[server_def.service_type.gate] = "gate",
	[server_def.service_type.center] = "center",
	[server_def.service_type.match] = "match",
	[server_def.service_type.pvp] = "pvp_room_",
	[server_def.service_type.social] = "social",
	[server_def.service_type.chat] = "chat",
}

function config_function.get_node_config()
	return node_config
end

function config_function.get_node_extern_info()
	return node_extern_info
end

function config_function.get_route_st_to_nt()
	return route_server_type_to_node_type
end

function config_function.get_node_service()
	return node_service
end

return config_function
