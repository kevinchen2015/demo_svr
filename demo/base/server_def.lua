
local server_def = {}

local service_type = {
	gate = 0,
	login = 1,
	heromanager = 2,
	playermanager = 3,
	tollgatemanager = 4,
	pvp = 5,
	yijimanager = 6,
	commentManager = 7,
	match = 15,
	room = 16,
	social = 17,
	chat = 18,
	center = 19,
	max = 20,
}

local node_type = {
	center_node = 1,
	gate_node = 2,
	--match_node = 3,
	room_group_node = 4,
	social_node = 5,
}


server_def.service_type = service_type
server_def.node_type = node_type
server_def.proxy_proto_id = 20

return server_def
