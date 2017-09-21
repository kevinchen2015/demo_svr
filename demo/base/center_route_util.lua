local skynet = require "skynet"
local node_util = require "static_node_util"

--只有中心服有这个center_routed!!!

local center_route_util = {}
local center_route

function center_route_util.send_to_node_by_type(des_node_type,des_service,...)
	return node_util.send_to_node_by_type(des_node_type,des_service,...)
end

function center_route_util.route_message(...)
	return skynet.send(center_route,"lua","route_message",...)
end

skynet.init(function()
	center_route = skynet.uniqueservice("center_routed")
end)

return center_route_util
