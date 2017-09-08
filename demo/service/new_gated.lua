local geteserver = require "snax.gateserver"
local crypt = require "skynet.crypt"
local skynet = require "skynet"
local server_def = require "server_def"
local debug_trace = require "debug_trace"
local service_util = require "service_util"

local gate_mc_channel
local server_handle = {}
local fd2agent = {}
local agent2fd = {}

local uid2fd_map = {}
local fd2uid_map = {}

local proxy = {}
local gate_cmd = {}
local free_agent_pool = {}


local function unbind_fd_uid_by_uid(uid)
	local fd = uid2fd_map[uid]
	uid2fd_map[uid] = nil
	if fd then
		fd2uid_map[fd] = nil
	end
end

local function unbind_fd_uid_by_fd(fd)
	local uid = fd2uid_map[fd]
	fd2uid_map[fd] = nil
	if uid then
		uid2fd_map[uid] = nil
	end
end

local function bind_fd_uid(fd,uid)

	unbind_fd_uid_by_uid(uid)

	uid2fd_map[uid] = fd
	fd2uid_map[fd] = uid
end

local function notify_agent_close(agent,fd)
	skynet.call(agent,"lua","on_close")
	table.insert(free_agent_pool,agent)
end

function on_user_login(uid,agent)
	gate_mc_channel:publish("on_user_login",uid,agent)
end

function on_user_logout(uid)
	gate_mc_channel:publish("on_user_logout",uid)
end

local function on_client_close(fd)

	local uid = fd2uid_map[fd]
	if uid ~= nil then
		on_user_logout(uid)
	end

	unbind_fd_uid_by_fd(fd)
	local agent = fd2agent[fd]
	if agent then
		print("agent set null,fd:"..fd)
		agent2fd[agent] = nil
		fd2agent[fd] = nil
		notify_agent_close(agent,fd)
	end
end

local function client_close(fd)
	geteserver.closeclient(fd)
	on_client_close(fd)
end

--------------------------------------------------------------

function server_handle.connect(fd,msg)
	print("connect:"..fd.."|"..msg)
	geteserver.openclient(fd)
	local agent = nil
	if #free_agent_pool > 0 then
		agent = free_agent_pool[1]
		table.remove(free_agent_pool,1)
	else
		agent = skynet.newservice("new_agentd")
	end
	fd2agent[fd] = agent
	agent2fd[agent] = fd

	skynet.call(agent,"lua","init",fd,skynet.self())
	print("fd:"..fd.." set to fd2agent")
end

function server_handle.disconnect(fd)
	print("disconnect:"..fd)
	on_client_close(fd)
end

function server_handle.message(fd,msg,sz)
	print("message:"..fd.." msg size:"..sz)
	local agent = fd2agent[fd]
	if agent == nil then
		client_close(fd)
		return
	end
	skynet.redirect(agent, 0, "proxy", 0, msg,sz)
end

function server_handle.error(fd,msg)
	print("error:"..fd.."|"..msg)
	client_close(fd)
end

function server_handle.warning(fd,size)
	print("warning:"..fd.."|"..size)
end

function server_handle.command(cmd,address,...)
	print("lua command:"..cmd)
	gate_cmd[cmd](...)
end
--------------------------------------------------------------------

function gate_cmd.time_out(fd)
	client_close(fd)
end

function gate_cmd.regist_proxy(id,service)
	print("gate_cmd.regist_proxy("..id)

	if proxy[id] == nil then
		proxy[id] = {}
	end
	table.insert( proxy[id], service )
end

function gate_cmd.login_rsp(err,agent,acc,uid,token)
	if agent2fd[agent] == nil then
		print("agent error!")
		return
	end

	local fd = agent2fd[agent]
	if err ~= 0 then
		print("login err!")
		
		if agent ~= nil then
			skynet.call(agent,"lua","send","login_rsp",err)
		end
		client_close(fd)
	else
		print("login sucess, agent:"..agent)
		for k,v in pairs(fd2agent) do
			print(k,v)
		end
		--print(agent)
		if agent ~= nil then
			if fd2uid_map[fd] ~= uid then
				local last_fd = uid2fd_map[uid]
				if last_fd ~= nil then
					client_close(last_fd)
				end
			end

			bind_fd_uid(fd,uid)
			skynet.call(agent,"lua","on_login",uid,acc,token)

			on_user_login(uid,agent)
		else
			print("agent is error!")
			client_close(fd)
		end
	end
end

function gate_cmd.write_proxy()
	service_util.sharedate_update("proxy_info",proxy)
end

function gate_cmd.on_self_start_finished()
	service_util.sharedate_create("gate_service",{skynet.self()})
	gate_mc_channel = service_util.create_mc("gate_mc_channel")
	service_util.sharedate_create("proxy_info",{})
end

function gate_cmd.on_server_start_finished()
	gate_cmd.write_proxy()
	gate_mc_channel:publish("on_start_finished")
end

--------------------------------------------------------------------------

skynet.register_protocol({
	name = "client",
	id = skynet.PTYPE_CLIENT,
})

skynet.register_protocol {
	name = "proxy",
	id = server_def.proxy_proto_id,
	pack = skynet.pack
}

geteserver.start(server_handle)





