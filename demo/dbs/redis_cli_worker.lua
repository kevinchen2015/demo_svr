local skynet = require "skynet"
local redis = require "skynet.db.redis"
local debug_trace = require "debug_trace"
local string_util = require "string_util"
local queue = require "skynet.queue"
local config = {}
local CMD = {}
local redis_db
local lock

--具体的redis链接保持和执行处理

function CMD.open(source,conf)
	config = conf
	redis_db = redis.connect(config)

	--todo reconnect
end

function do_request(req_info)
	local time_now = skynet.time()
	if time_now - req_info.time > 50 then
		return "time_out"
	end
	local cmd = req_info.cmd
	local param = req_info.param
	local ret = redis_db[cmd](redis_db,string_util.split(param," "))
	return ret
end

function CMD.execute(source,cmd,param,time)
	req_info.cmd = cmd
	req_info.param = param
	req_info.time = time
	local ret = lock(do_request,req_info)
	return ret
end

skynet.start(function()

	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)

	lock = queue()
end)



