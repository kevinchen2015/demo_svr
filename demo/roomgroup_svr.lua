local skynet = require("skynet")


skynet.start(function()
    print("roomgroup_svr start...")
    
    skynet.uniqueservice("static_noded")
    skynet.uniqueservice("room_groupd")

    skynet.exit()
end)