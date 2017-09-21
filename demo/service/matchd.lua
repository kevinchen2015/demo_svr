local skynet = require "skynet"
local room_manager_util = require "room_manager_util"
local node_util = require "static_node_util"
local server_def = require "server_def"
local center_route_util = require "center_route_util"
local sprotoloader = require "sprotoloader"
local service_util = require "service_util"
local package_proto = require "package.proto"
local debug_trace = require "debug_trace"
local server_error = require "server_error"

local command = {}
local room_group_info = {}
local room_gourp_status = {}
local room_group_proxy = {}
local match_pool = {}


local request_cmd = {}
local running = true
local self_type
local self_id

local proto
local proto_host
local proto_request

local timeout_ = 5

--匹配服务，目前挂在中心服节点上

--简单演示1v1 匹配
-------------------------------------------------------------------

function command.ping(source,...)
	return ...
end

function command.add_to_match_pool(source,user_id)
	local ret = server_error.id.ok
	local user_room_info = room_manager_util.query_user_room_info(user_id)

	if user_room_info then
		ret = server_error.id.room_lock_in_room
	end

	if ret == server_error.id.ok then
		for i,v in ipairs(match_pool) do
			if v.uid == user_id then
				ret = server_error.id.match_already_matching
				break
			end
		end
	end
	if ret == server_error.id.ok then
		table.insert(match_pool,{uid=user_id,addr=source,time=skynet.time()})
	end
	return ret
end 

function command.remove_from_match_pool(source,uid)
	local ret = server_error.id.match_not_in_match_pool
	for i,v in ipairs(match_pool) do
		if v == uid then
			ret = server_error.id.ok
			break
		end
	end
	return ret
end

-------------------- decode msg -------------------------------------------------
function command.on_route_message(source,msg,sz,uid)
	print(sz)

	local head = {}
	local body = nil
	body,head.des_type,head.des_id,head.src_type,head.src_id = package_proto.decode(msg,sz)
	
	--print(body)
	local fn = function(head,type,name,args,response)
		if type == "REQUEST" then
		
			local f = request_cmd[name]
			if f then
				local r = f(source,uid,head,args)
				if r == nil then
					return nil
				end
				if response then
            		return response(r)
        		end
			end
		end
	end
	local m = fn(head,proto_host:dispatch(body,body:len()))
	if m then
		local message = package_proto.encode(m,m:len(),head.src_type,head.src_id,self_type,self_id)
		center_route_util.route_message(message,message:len(),uid)
	else 
	end
end

function request_cmd.match_request(source,uid,head,args)
	print("match_request,uid:"..uid)
	return {ret = command.add_to_match_pool(source,uid)}
end

function request_cmd.match_cancle(source,uid,head,args)
	return {ret = command.remove_from_match_pool(source,uid)}
end

------------------------------------------------------------------------

function make_match(uids)
	--print("make_match!")
	local r,gid,rid = room_manager_util.push_to_idle_room(uids)
	local msg = proto_request("match_result", { ret = r, group_id=gid,room_id=rid,uid_list = uids})
	local message = package_proto.encode(msg,msg:len(),server_def.service_type.gate,0,self_type,self_id)
	for i,uid in ipairs(uids) do
		center_route_util.route_message(message,message:len(),uid)
	end
end

function match_timeout(uid)
	local msg = proto_request("match_result", { ret = server_error.id.match_timeout})
	local message = package_proto.encode(msg,msg:len(),server_def.service_type.gate,0,self_type,self_id)
	center_route_util.route_message(message,message:len(),uid)
end

function match_worker()
	while running do
		while #match_pool > 1 do
			local uid_list = {}
			local uid1 = match_pool[1]
			table.remove(match_pool,1)
			local uid2 = match_pool[1]
			table.remove(match_pool,1)
			table.insert(uid_list,uid1.uid)
			table.insert(uid_list,uid2.uid)
			skynet.fork(make_match,uid_list)  
		end

		local time_now = skynet.time()
		local index = 1
		while #match_pool >= index do
			 if time_now - match_pool[index].time > timeout_ then
				skynet.fork(match_timeout,match_pool[index].uid)
				table.remove(match_pool,index)
			 else
				index = index + 1
			 end
		end
		skynet.sleep(100)
	end
end

skynet.start(function()
	
	self_type = server_def.service_type.match
 	self_id = node_util.query_self_node_id()
	print("self_type:"..self_type.." self_id:"..self_id)

	local proto_id = 1
	sprotoloader.register("./work/sproto/match_proto.sproto",proto_id)
	proto = sprotoloader.load(proto_id)
	proto_host = proto:host "package"
    proto_request = proto_host:attach(proto)

	skynet.dispatch("lua", function(session, source, cmd, ...)
		local f = assert(command[cmd])
		skynet.ret(skynet.pack(f(source, ...)))
	end)

	skynet.fork(match_worker)
end)
