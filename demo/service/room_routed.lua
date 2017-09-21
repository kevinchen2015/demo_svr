local skynet = require "skynet"
local server_def = require "server_def"
local node_util = require "static_node_util"
local debug_trace = require "debug_trace"
local room_def = require "room_def"

local command = {}
local uid2room = {}

--gate 上 uid的玩家根据这个信息路由房间内的pvp游戏

function command.ping(source,...)
	return ...
end

function command.route_to_room(source,msg,sz,uid)
	local room_info = uid2room[uid]
	if room_info == nil then
		return 1
	end
	local rsn = node_util.query_remote_server_name(room_info.gname,room_info.rname)
	node_util.send(room_info.gname,rsn,"on_route_message",msg,sz,uid)
	return 0
end

function command.notify_room_locked(source,group_id,room_id,uid_list)
	print("notify_room_locked")
    for i,v in ipairs(uid_list) do
		group_node_name = node_util.query_node_name_by_id(group_id)
		room_name = room_def.room_service_prefix..i
		uid2room[v] = {gid = group_id,rid = room_id,gname = group_node_name,rname = room_name}
	end
	command.route_to_room(source,"1",1,11)
end

function command.notify_room_unlock(source,group_id,room_id,uid_list)
	print("notify_room_unlock")
	for i,v in ipairs(uid_list) do
		uid2room[v] = nil
	end
end

skynet.start(function()
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
end)
