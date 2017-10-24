

local skynet = require "skynet"



local node_info_mgr = {}


function node_info_mgr.update_node_info(path,node_info,has_child)
	local old_node_info = node_info_mgr[path]
	if old_node_info == nil then
		old_node_info = {}
		node_info_mgr[path] = old_node_info
	end

end


return node_info_mgr