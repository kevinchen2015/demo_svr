local skynet = require "skynet"
local node_util = require "static_node_util"
local room_def= require "room_def"

skynet.start(function()
    print("center_svr start...")
    
    local center_route = skynet.uniqueservice("center_routed")
    node_util.register("center_route",center_route)

    local room_manager = skynet.uniqueservice("room_managerd")
    node_util.register(room_def.room_manager_service_name,room_manager)

    local match = skynet.uniqueservice("matchd")
    node_util.register("match",match)

    skynet.exit()
end)