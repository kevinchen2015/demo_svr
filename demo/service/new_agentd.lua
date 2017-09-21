local skynet = require "skynet"
local socket = require "skynet.socket"
local proto = require "package.proto"
local server_def = require "server_def"
local service_util = require "service_util"		
local sprotoloader = require "sprotoloader"
local node_util = require "static_node_util"

local user_id = 0
local user_acc = nil
local user_token = nil
local fd = nil
local gate
local proxy = {}
local CMD = {}


local center_node = "center_node"
local room_route

local self_gate_id = 0



local function reset()
	user_id = 0
	user_acc = nil
	user_token = nil
	fd = nil
	
end

local function agent_time_out()
	if fd == nil then
		return
	end

	if user_id == 0 then
		skynet.call(gate,"lua","time_out",fd)
		return
	else
		--todo heart time check
	end
	skynet.timeout(600, agent_time_out)
end

local function update_proxy()
	proxy = service_util.sharedate_query("proxy_info")

end

function CMD.init(source,_fd,_gate)
	reset()
	fd = _fd
	gate = _gate
	--update proxy
	update_proxy()

	--skynet.timeout(600, agent_time_out) -- 6s
end

function CMD.on_close(source)
	reset() --skynet.exit()   //not destory, back to pool
end

function CMD.on_login(source,uid,acc,token)
	user_id = uid
	user_acc = acc
	user_token = token
end

function CMD.get_login_info(source)
	return user_id,user_acc
end

function CMD.send(source, msg, src_type)
	if fd == nil then
		return
	end
	local message = proto.encode(msg,msg:len(),0,0,src_type,0,true)
	socket.write(fd, message)
end

--转发从别的node发来的编码过的消息
function CMD.send_raw(source,msg,size)
	if fd == nil then
		return
	end
	local message = string.pack(">s2",msg)
	socket.write(fd, message)
end




function on_self_msg(agent,msg,sz)
        local body,des_type,des_id,src_type,src_id  = proto.decode(msg,sz)
	heartbeat(body, body:len())
end

function get_router(des_type,uid)
	local service = proxy[des_type]
	if service then
		local index = 1
		if #service > 1 then
			index = math.fmod(uid,#service)
		end
		if index == 0 then
			index = 1
		end
		return service[index]
	end
	return nil
end

function on_proxy_message(session,source,msg,sz)
	
	local des_type,des_id,src_type,src_id = proto.decode_head(msg,sz)

	if des_type == 0 then
		on_self_msg(agent,msg,sz)
		return
	end
	--proxy msg
	local agent = skynet.self()
	local uid = user_id
	if uid ~= nil then
		local des = get_router(des_type, uid)
		if des ~= nil then
			skynet.redirect(des, agent, "proxy",fd,msg,sz)
		else
			--标记本地gate路由
			proto.modify_head(msg,sz,des_type,des_id,server_def.service_type.gate,self_gate_id)
			if des_type == server_def.service_type.pvp then   --特殊处理,动态路由,直连room
				if room_route == nil then
					room_route = skynet.uniqueservice("room_routed")
				end
				if room_route then
					local r = skynet.call(room_route,"lua","route_to_room",msg,sz,uid)
					if r ~=  0 then
						print("user have no room!")
					end
				end
			else
				local center_route = node_util.query_remote_server_name(center_node,"center_route")
				if center_route then
					node_util.send(center_node,center_route,"route_message",message,message:len(),uid)
				else
					print("error:can not find center_route!")
				end
			end
		end
	else
		if des_type == server_def.service_type.login then
			local des = get_router(des_type,uid)
			if des ~= nil then
				skynet.redirect(des, agent, "proxy",fd,msg,sz)
			end
		else
			skynet.call(gate, "lua", "time_out", fd)
		end
	end
end

skynet.start(function()

	self_gate_id = node_util.query_self_node_id()

	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)

	service_util.register_proxy_handle_custom(function(msg,size) return msg,size end,on_proxy_message)
end)
