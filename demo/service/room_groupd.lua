local skynet = require "skynet"
local room_def = require "room_def"
local node_util = require "static_node_util"

local command = {}

local room_group_id
local room_num

local room_info = {}


local match_node = "center_node"
local room_manager = room_def.room_manager_service_name


local manager
----------------------------------

function command.lock_room(source,group_id,room_id,uid_list)
	--print("lock_room ,groupid:"..group_id.." room_id:"..room_id)
	local ret = 0
	if group_id ~= room_group_id then
		return 2
	end
	if room_id > room_num or room_id < 1 then
		return 3
	end
	room = room_info[room_id]
	ret = skynet.call(room.addr,"lua","lock",uid_list)
	if ret == 0 then
		room.lock_state = room_def.room_lock_state.locked
		room.uid_list = uid_list
	end
	return ret
end

function command._unlock_room_req(source,room_id)
	print("_unlock_room_req ,"..room_id)
	room = room_info[room_id]
	room.lock_state = room_def.room_lock_state.idle
	node_util.send(match_node,manager,"on_recv_room_unlocked",room_group_id,room_id,room.uid_list)
	room.uid_list = {}
end

function command.query_all_group_info(source)
	local group_info = {id = room_group_id,room_info}
	skynet.send(source,"lua","on_recv_group_report",node_util.get_self_node_name(),group_info)
end

function command.proxy_to_room(source,room_id,func,uid)
	local r =room_info[room_id]
	skynet.redirct(r.addr,source,"lua",0,func,uid)
end

---------------------------------------

function init_room()
	for i=1,room_num,1 do
		local room = {}
		room.addr = skynet.newservice("roomd")
		room.i = i
		room.lock_state = room_def.room_lock_state.idle
		room.uid_list = {}
		room.name = room_def.room_service_prefix..i
		table.insert(room_info,room)
		skynet.call(room.addr,"lua","init",skynet.self(),i)
		node_util.register(room.name,room.addr)
	end
end

skynet.start(function()

	room_group_id = node_util.query_self_node_id()
	room_num = tonumber(skynet.getenv("room_num"))
	
	assert(room_group_id)
	assert(room_num)
	print("room num:"..room_num)


	skynet.dispatch("lua", function(session, source, cmd, ...)
		local f = command[cmd]
		if f then 
			skynet.ret(skynet.pack(f(source, ...)))
		else
			print("error ! command:"..cmd)
			assert(f)
			skynet.ret(skynet.pack(nil))
		end
	end)

	init_room()

	node_util.register(room_def.room_group_service_name,skynet.self())
	manager = node_util.query_remote_server_name(match_node,room_manager)
	local group_info = {id = room_group_id,rooms = room_info}
	node_util.call(match_node,manager,"add_group",node_util.get_self_node_name(),group_info)

end)
