local skynet = require "skynet"
local socket = require "skynet.socket"
local httpd = require "http.httpd"
local sockethelper = require "http.sockethelper"
local urllib = require "http.url"

local string_util = require "string_util"

local table = table
local string = string

local mode = ...


local path_router = {}
local CMD = {}

------------------------------------------------------------

if mode == "agent" then

local function _get_router(path)
	for k, v in pairs(path_router) do
		local index_start,index_end = string.find(path,k)
		if index_start then
			return v
		end
	end
	return nil
end

function respone_default(error_string)
	local tmp = {}
	table.insert(tmp, error_string)
	return 505,tmp
end

--[[
function request_handler_test(code,path,query,method,header,body)
	local myext = ""
	local tmp = {}
	if header.host then
		table.insert(tmp, string.format("host: %s", header.host))
	end
	table.insert(tmp, string.format("path: %s", path))
	if query then
		local q = urllib.parse_query(query)
		for k, v in pairs(q) do
			table.insert(tmp, string.format("query: %s= %s", k,v))
		end
	end
	table.insert(tmp, "-----header----")
	for k,v in pairs(header) do
		table.insert(tmp, string.format("%s = %s",k,v))
	end
	table.insert(tmp, "++++++body++++++\n" .. body)
	table.insert(tmp, "------ext-----\n" .. myext)
	local index_start,index_end = string.find(path,"/gonggao")
	if index_start then
		table.insert(tmp, "yes!\n")
	else
		table.insert(tmp, "no!\n")
	end
	local path_table = string_util.split(path,"/")
	for k,v in pairs(path_table) do
		table.insert(tmp, string.format("%s = %s",k,v))
	end
	return code,tmp
end
]]--

local function response(id, ...)
	local ok, err = httpd.write_response(sockethelper.writefunc(id), ...)
	if not ok then
		-- if err == sockethelper.socket_error , that means socket closed.
		skynet.error(string.format("fd = %d, %s", id, err))
	end
end

function CMD.regist_router(source,path,service)
	path_router[path] = service
end

function CMD.on_http_request(source,id)
	socket.start(id)
	-- limit request body size to 8192 (you can pass nil to unlimit)
	local code, url, method, header, body = httpd.read_request(sockethelper.readfunc(id), 8192)
	if code then
		if code ~= 200 then
			response(id, code)
		else
			local path, query = urllib.parse(url)
			--local rsp_code,tmp = request_handler_test(code,path,query,method,header,body)
			
			local rsp_code,tmp = respone_default("can not router request!")
			local service = _get_router(path)
			if service then
				local q = nil
				if query then
					q = urllib.parse_query(query)
				end
				rsp_code,tmp = skynet.call(service,"lua","request",code,path,q,method,header,body)
			end
			response(id, code, table.concat(tmp,"\n"))
		end
	else
		if url == sockethelper.socket_error then
			skynet.error("socket closed")
		else
			skynet.error(url)
		end
	end
	socket.close(id)
end

skynet.start(function()

	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)
		
end)
-----------------------------------------------
else
----------------------------------------------------
local agent = {}

function CMD.regist_router(source,path,service)
	for i,v in ipairs(agent) do
		skynet.call(v,"lua","regist_router",path,service)
	end
end

skynet.start(function()
	
	for i= 1, 8 do
		agent[i] = skynet.newservice(SERVICE_NAME, "agent")
	end
	local balance = 1
	local id = socket.listen("0.0.0.0", 9820)
	skynet.error("Listen web port 9820")
	socket.start(id , function(id, addr)
		skynet.error(string.format("%s connected, pass it to agent :%08x", addr, agent[balance]))
		skynet.send(agent[balance], "lua","on_http_request",id)
		balance = balance + 1
		if balance > #agent then
			balance = 1
		end
	end)

	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)
	
end)

end
