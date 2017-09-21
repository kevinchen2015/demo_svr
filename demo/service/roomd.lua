local skynet = require "skynet"
local sprotoloader = require "sprotoloader"
local queue = require "skynet.queue"		--逻辑保证强顺序性，就引入这个

local game_interface = require "battle_logic"  --game logic to implement!

local command = {}
local parent_service
local room_id

local is_lock
local uid_list = {}

local room_interface = {}    --room interface

---------------------------------------------------------------------

function room_interface.unlock()
	print("room unlock ,room id:"..room_id)
	game_interface.on_exit(uid_list)
	uid_list = {}
	skynet.call(parent_service,"lua","_unlock_room_req",room_id)
end

----------------------------------------------------------------------

function command.init(source,parent,id)
	parent_service = parent
	room_id = id
end

function command.lock(source,uids)
	print("room lock ,room id:"..room_id)
	is_lock = true
	uid_list = uids
	game_interface.on_enter(uids)
	return 0
end

function command.enter(source,uid)
	--todo 玩家登入，从客户端发登录消息,作为客户端第一次链接发的逻辑协议
	--用来判断是否需要断线重连刷数据
	
end

------------------------------------------------------------

--from other node todo !房间里的消息要保序
function command.on_route_message(source,msg,sz,uid)
	--decode
	print("room recv client battle msg!")
end

skynet.start(function()

	skynet.dispatch("lua", function(session, source, cmd, ...)
		local f = assert(command[cmd])
		skynet.ret(skynet.pack(f(source, ...)))
	end)

	game_interface.set_room_interface(room_interface)
	game_interface.on_init()

end)
