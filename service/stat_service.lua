local skynet = require "skynet"
local JSON 	 = require "JSON"
local httpd = require "http.httpd"
local sockethelper = require "http.sockethelper"
local pb = require "protobuf"
local memory = require "skynet.memory"

local CMD = {}
local requst_handle = {}


local function response(id, ...)
	local ok, err = httpd.write_response(sockethelper.writefunc(id), ...)
	if not ok then
		-- if err == sockethelper.socket_error , that means socket closed.
		skynet.error(string.format("fd = %d, %s", id, err))
	end
end

local function get_stat()
	return skynet.call(".launcher", "lua", "STAT")
end

local function get_mem()
	return skynet.call(".launcher", "lua", "MEM")
end

local function get_task_by_service()
	--todo
end

local function get_cmem()
	local info = memory.info()
	local tmp = {}
	for k,v in pairs(info) do
		tmp[skynet.address(k)] = v
	end
	tmp.total = memory.total()
	tmp.block = memory.block()
	return tmp
end

function requst_handle.GET(query,path,body,conn_id,code)
	local respone_table = {error = 0}
	local ret_code = 200
	if body:len()>0 then
		local request_table = JSON:decode(body)
		--print("request cmd:"..request_table.cmd)
		if request_table.cmd == "show_all" then
			respone_table.stat = get_stat()
			respone_table.mem = get_mem()
			respone_table.cmem = get_cmem()
		else
			respone_table.error = 3
			respone_table.reason = "cmd is error!"
		end		
	else
		ret_code = 404
		respone_table.error = 2
		respone_table.reason = "param is error"
	end
	local respone_str = JSON:encode(respone_table)
	response(conn_id,ret_code,respone_str)
end

--function requst_handle.POST(query,path,body,conn_id,code)
--end

--[[
local check()
	--todo
end
function handler_mainloop()
	while true do
		check()
		skynet.sleep(100)  --1 second
	end
end
]]--

--------------------------------------------------------------
function CMD.request(source,code,path,query,method,header,body,conn_id)
	local action = method
	local f = requst_handle[action]
	if f then
		f(query,path,body,conn_id,code)
	else
		local respone_table = {error = 1,reason = "request can't finded!"}
		local respone_str = JSON:encode(respone_table)
		response(conn_id,ret_code,respone_str)
	end
end

skynet.start(function()

	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)
	--skynet.fork(handler_mainloop)

end)
