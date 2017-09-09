local skynet = require "skynet"
local sprotoloader = require ("sprotoloader")
local crypt = require "skynet.crypt"
local server_def = require "server_def"
local debug_trace = require "debug_trace"
local service_util = require "service_util"
--local pb = require "protobuf"


local gate
local CMD = {}
local request_cmd = {}
local gate_mc_subscribe = {}

local test_uid_counter = 1

local proto
local proto_host

local user2id = {}
local id2userinfo = {}

local gate_mc_channel

--------------------------------------
function on_login(uid,user,token)
	user2id[user] = uid
	id2userinfo[uid] = {u=user,t=token,}
end

function mc_on_logout(source,uid)
	 local userinfo = id2userinfo[uid]
	 if userinfo ~= nil then
	 	user2id[userinfo.u] = nil
		id2userinfo[uid] = nil
	 end
end
-------------------------------------------------------------------
--login
function request_cmd.login(source,head,args,response,ud)
	local token = args.client_md5
	local err = 0
	local uid = 0

	--todo load
	local user = args.client_md5
	local i = user2id[user]
	if i ~= nil then
		local rsp = {ret=0,uid=i,}
		local rsp_msg = response(rsp,ud)
		skynet.send(source,"lua","send",rsp_msg,server_def.service_type.login)
		return
	end

    local rsp = {ret=0,uid=0,}

	if err == 0 then
		test_uid_counter = test_uid_counter + 1
		uid = test_uid_counter
		rsp.uid = tostring(uid)
	else
		rsp.uid = tostring(0)
	end
	rsp.ret = err

	if err == 0 then
		on_login(uid,user,token)
	end

	--notify gate
	skynet.send(gate,"lua","login_rsp",err,source,user,uid,token)
	--notify client
	local rsp_msg = response(rsp,ud) 
	skynet.send(source,"lua","send",rsp_msg,server_def.service_type.login)

end

skynet.start(function()
	--lua
	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(session,source,...)))
	end)

	local t = service_util.sharedate_query("gate_service")
	gate = t[1]

	--proto and proxy
	proto = sprotoloader.load(1)
	proto_host = proto:host "package"

	service_util.register_proxy_handle_sproto(proto_host,request_cmd,nil)

	--gate mc handler
	gate_mc_subscribe = service_util.subscribe_mc("gate_mc_channel")
	service_util.add_subscribe_handler(gate_mc_subscribe,"on_user_logout",mc_on_logout)

end)
