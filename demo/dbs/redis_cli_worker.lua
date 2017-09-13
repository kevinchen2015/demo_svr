local skynet = require "skynet"
local redis = require "skynet.db.redis"
local pb = require "protobuf"
local debug_trace = require "debug_trace"
local string_util = require "string_util"
local config = {}
local CMD = {}
local redis_db
local requst_handle = {}
local rsp_name2id = {}

--具体的redis链接保持和执行处理

function CMD.open(source,conf)
	config = conf
	redis_db = redis.connect(config)

	--todo reconnect
end

function CMD.execute(source,cmd,param)
	local ret = redis_db[cmd](redis_db,string_util.split(param," "))
	return ret
end

skynet.start(function()

	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)

end)



