local skynet = require "skynet"
local socket = require "skynet.socket"
local proto = require "package.proto"
local server_def = require "server_def"
local service_util = require "service_util"		

local user_id = 0
local user_acc = nil
local user_token = nil
local fd = nil
local gate
local proxy = {}
local CMD = {}

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
		print("agent login time out")
		print(gate)
		skynet.call(gate,"lua","time_out",fd)
		return
	else
		--todo heart time check
		print("heart time check!,fd:"..fd)
	end
	skynet.timeout(600,agent_time_out)
end

local function update_proxy()
	proxy = service_util.sharedate_query("proxy_info")
end

function CMD.init(source,_fd,_gate)
	reset()

	fd = _fd
	gate = _gate
	print("agnet.init:"..skynet.self().." fd:"..fd)
	
	--update proxy
	update_proxy()

	skynet.timeout(600,agent_time_out) -- 6s
end

function CMD.on_close(source)
	print("fd on_close:"..fd)
	reset() --skynet.exit()   //not destory, back to pool
end

function CMD.on_login(source,uid,acc,token)
	user_id = uid
	user_acc = acc
	user_token = token
	print("uid:"..user_id.." is login!")
end

function CMD.get_login_info(source)
	return user_id,user_acc
end

function CMD.send(source,msg,src_type)
	if fd == nil then
		return
	end

	local message = proto.encode(msg,msg:len(),0,0,src_type,0)
	print("agent:"..skynet.self().." send data,size:"..message:len())
	socket.write(fd, message)
end

function on_self_msg(agent,msg,sz)
	
end

function get_router(des_type,uid)
	local service = proxy[des_type]
	if service then
		local index = 1
		if #service > 1 then
			index = uid%#service
		end
		if index == 0 then
			index = 1
		end
		return service[index]
	else
		print("err:proxy not find service,des_type"..des_type)
	end
	return nil
end

function on_proxy_message(session,source,msg,sz)
	
	local body,des_type,des_id,src_type,src_id = proto.decode(msg,sz)

	if des_type == 0 then
		on_self_msg(agent,body,body:len())
		return
	end

	--proxy msg
	local agent = skynet.self()
	local uid = user_id
	if uid ~= nil then
		local des = get_router(des_type,uid)
		if des ~= nil then
			skynet.redirect(des, agent, "proxy",fd,body,body:len())
		end
	else
		if des_type == server_def.service_type.login then
			local des = get_router(des_type,uid)
			if des ~= nil then
				--skynet.send(des,"lua","on_message",body,body:len())
				skynet.redirect(des, agent, "proxy",fd,body,body:len())
			end
		else
			skynet.call(gate,"lua","time_out",fd)
		end
	end
end

skynet.start(function()
	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)

	service_util.register_proxy_handle_custom(function(msg,size) return msg,size end,on_proxy_message)
end)
