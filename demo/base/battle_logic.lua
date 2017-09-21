local skynet = require "skynet"


local game_interface = {}

local function test_unlock_room()
	game_interface.room_inter.unlock()
end
--------------------------------------

function game_interface.on_init()

end

function game_interface.on_enter(uids)
	--todo load user game info 

	--test unlock when game over 
	skynet.timeout(300,test_unlock_room)
end

function game_interface.on_exit(uids)

end

function game_interface.set_room_interface(interface)
	game_interface.room_inter = interface
end


return game_interface