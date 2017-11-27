local skynet = require "skynet"
local socket = require "skynet.socket"
local httpd = require "http.httpd"
local sockethelper = require "http.sockethelper"
local urllib = require "http.url"
local queue = require "skynet.queue"
local mode = ...
local path_router = {}
local CMD = {}

if mode == "agent" then


----------------------------http agent  start------------------
local lock = queue()

local function _get_router(path,id)
	for k, v in pairs(path_router) do
		local index_start,index_end = string.find(path,k)
		if index_start then
			local index = math.fmod(id,#v)
			index = index+1
			return v[index]
		end
	end
	return nil
end

function respone_default(error_string)
	local tmp = {}
	table.insert(tmp, error_string)
	return 505,tmp
end

local function response(id, ...)
	local ok, err = httpd.write_response(sockethelper.writefunc(id), ...)
	if not ok then
		-- if err == sockethelper.socket_error , that means socket closed.
		skynet.error(string.format("fd = %d, %s", id, err))
	end
end

function CMD.regist_router(source,path,service)
	if path_router[path] == nil then
		path_router[path] = {}
	end
	table.insert(path_router[path],service)
end

function do_http_request(source,id,time)
	local time_now = skynet.time()
	if time_now - time > 50 then
		respone_default("request timeout!")
		socket.close(id)
		return
	end
	
	socket.close(id)
	socket.start(id)
	-- limit request body size to 8192 (you can pass nil to unlimit)
	local code, url, method, header, body = httpd.read_request(sockethelper.readfunc(id), 8192)
	if code then
		if code ~= 200 then
			response(id, code)
		else
			local path, query = urllib.parse(url)
			--local rsp_code,tmp = request_handler_test(code,path,query,method,header,body)
			local tmp = {}
				if header.host then
					table.insert(tmp, string.format("host: %s", header.host))
					--print(string.format("host: %s", header.host))
				end		
			--local rsp_code,tmp = respone_default("can not router request!")
			local service = _get_router(path,id) 	--nil
			if service then
				local q = nil
				if query then
					q = urllib.parse_query(query)
				end
				skynet.call(service,"lua","request",code,path,q,method,header,body,id)
			end
			--response(id, code, table.concat(tmp,"\n"))
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

function CMD.on_http_request(source,id,time)
	lock(do_http_request,source,id,time)
end

skynet.start(function()
	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)
		
	lock = queue()
end)

----------------------------http agent  end-----------------------

else

----------------------------http server start--------------------

local agent = {}

function CMD.regist_router(source,path,service)
	for i,v in ipairs(agent) do
		skynet.call(v,"lua","regist_router",path,service)
	end
end

skynet.start(function()	
	for i= 1, 4 do
		agent[i] = skynet.newservice(SERVICE_NAME, "agent")
	end
	local balance = 1

	local id = socket.listen("0.0.0.0", 9912)
	skynet.error("Listen web port 9912")

	socket.start(id , function(id, addr)
		skynet.send(agent[balance], "lua","on_http_request",id,skynet.time())
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


----------------------------http server end--------------------

end
