local skynet = require "skynet"
local string_util = require "string_util"

local CMD = {}
local request_cmd = {}
local action_handler = {}

function action_handler.notify_add(q)
	if q == nil then
		return
	end
	print("notify_add")
	for k, v in pairs(q) do
		print(k..":"..v)
	end
end

function action_handler.notify_del(q)
	if q == nil then
		return
	end
	print("notify_del")
	for k, v in pairs(q) do
		print(k..":"..v)
	end
end

function handler_mainloop()
	while true do
		if #request_cmd > 0 then
			local cmd = request_cmd[1]
			table.remove(request_cmd,1)

			--do cmd
			local action = cmd[1]
			local query = cmd[2]
			local f = action_handler[action]
			f(query)
		end
		skynet.sleep(2)
	end
end

function CMD.request(source,code,path,query,method,header,body)
	local tmp = {}
	local path_table = string_util.split(path,"/")
	if #path_table < 3 then
		table.insert(tmp, "action param error!")
		return 404,tmp
	end

	local action = path_table[3]
	local f = action_handler[action]
	if f then
		table.insert(request_cmd,{action,query})
	else
		table.insert(tmp, "action not find!")
		return 404,tmp
	end
	table.insert(tmp, "OK")
	return 200,tmp
end

skynet.start(function()
	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)

	skynet.fork(handler_mainloop)
end)
