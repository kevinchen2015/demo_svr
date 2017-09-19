local skynet = require "skynet"
local mc = require "skynet.multicast"
local datasheet_builder = require "skynet.datasheet.builder"
local datasheet = require "skynet.datasheet"
local server_def = require "server_def"
local sprotoloader = require "sprotoloader"
local datacenter = require "skynet.datacenter"
local package_proto = require "package.proto"

local service_util = {}
local uid2agent = {}
local agent2uid = {}
local uid2info = {}


local function _on_login(source,uid,agent)
	uid2agent[uid] = agent
	agent2uid[agent] = uid
end

local function _on_logout(source,uid)
	local agent = uid2agent[uid]
	if agent then
		agent2uid[agent] = nil
	end
	uid2agent[uid] = nil
	uid2info[uid] = nil
end

function service_util.set_uid_info(uid,info)
	uid2info[uid] = info
end

function service_util.get_info_by_uid(uid)
	return uid2info[uid]
end

function service_util.get_agent_by_uid(uid)
	return uid2agent[uid]
end

function service_util.get_uid_by_agent(agent)
	return agent2uid[agent]
end

function service_util.sharedate_create(key,value)
	datasheet_builder.new(key,value)
end

function service_util.sharedate_update(key,value)
	datasheet_builder.update(key,value)
end

function service_util.sharedate_query(key)
	return datasheet.query(key)
end

function service_util.datacenter_set(k,v)
	datacenter.set(k,v)
end

function service_util.datacenter_get(k)
	return datacenter.get(k)
end

function service_util.subscribe_mc(mc_name)
	--local c = service_util.datacenter_get(mc_name)
	local c = service_util.sharedate_query(mc_name)[1]
	local subscribe_info = {}
	subscribe_info.ch = mc.new {
		channel = c,
		dispatch = function(channel,source,cmd,...)
			local t_fn = subscribe_info.mc_handler[cmd]
			if t_fn ~= nil then
				for i,v in ipairs(t_fn) do 
					v(source,...)
				end
			end
		end
	}
	subscribe_info.ch:subscribe()
	subscribe_info.mc_handler = {}
	if mc_name == "gate_mc_channel" then
		local fn = {}
		table.insert(fn,_on_login)
		subscribe_info.mc_handler["on_user_login"] = fn
		fn = {}
		table.insert(fn,_on_logout)
		subscribe_info.mc_handler["on_user_logout"] = fn
	end
	return subscribe_info
end

function service_util.add_subscribe_handler(info,cmd,fn)
	local old_fn = info.mc_handler[cmd]
	if old_fn == nil then
		old_fn = {}
	end
	table.insert(old_fn,fn)
	info.mc_handler[cmd] = old_fn
end

function service_util.create_mc(mc_name)
	local c = mc.new()
	--service_util.datacenter_set(mc_name,c.channel)
	service_util.sharedate_create(mc_name,{c.channel})
	return c
end

function service_util.register_proxy_handle_custom(upack_fn,dispatch_fn)
	skynet.register_protocol {
	name = "proxy",
	id = server_def.proxy_proto_id,
	pack = skynet.pack,
	unpack = function(msg,size)
		return upack_fn(msg,size)
	end,
	dispatch = dispatch_fn
	}
end

function service_util.register_proxy_handle_sproto(sproto_host,requst_handle,response_handle)
	local sproto_host_dispatch = function(msg,sz)
		local head = {}
		local body = nil
		body,head.des_type,head.des_id,head.src_type,head.src_id = package_proto.decode(msg,sz)
		return head,sproto_host:dispatch(body,body:len())
	end
	service_util.register_proxy_handle_custom(sproto_host_dispatch,function(session,source,head,type,name,...)
		if type == "REQUEST" then
			local fn = requst_handle[name]
			if fn then
				fn(source,head,...)
			end
		else
			local fn = response_handle[name]
			if fn then
				fn(source,head,...)
			end
		end
	end)
end

return service_util
