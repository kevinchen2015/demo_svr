
local server_def = {}

local service_type = {
	gate = 0,
	login = 1,

	max = 10,
}

server_def.service_type = service_type
server_def.proxy_proto_id = 20

return server_def
