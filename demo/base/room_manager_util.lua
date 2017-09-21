local skynet = require "skynet"

local room_manager_util = {}

local manager

function room_manager_util.push_to_idle_room(uid_list)
	return skynet.call(manager,"lua","lock_idle_room",uid_list)
end

function room_manager_util.add_listener(addr)
	return skynet.call(manager,"lua","add_listener",addr)
end

function room_manager_util.remove_listener(addr)
	return skynet.call(manager,"lua","remove_listener",addr)
end

function room_manager_util.query_user_room_info(uid)
	return skynet.call(manager,"lua","query_user_room_info",uid)
end

skynet.init(function()
	manager = skynet.uniqueservice("room_managerd")
end)

return room_manager_util
