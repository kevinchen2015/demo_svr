local skynet = require "skynet"
local string_util = require "string_util"
local JSON 	 = require "JSON"
local redis = require "skynet.db.redis"
local httpd = require "http.httpd"
local sockethelper = require "http.sockethelper"
local pb = require "protobuf"
local debug_trace = require "debug_trace"


local CMD = {}
local requst_handle = {}

local redis_db	= nil

local NOTICE_REDIS_ID 		= "xgameNoteId"			--不重复的id的key(xgameNoteId:0)
local NOTICE_ID_KEY 		= "xgameNoteIdKey"  	--endTime:id的key(xgameNoteIdKey endTime id)
local ID_HEAD 				= "xgameNoteId_"		--id的头(xgameNoteId_1,xgameNoteId_2,...)
local NOTICE_VERSION_KEY 	= "xgameNoteVersionKey" --redis存储版本的key
local NOTICE_BASE_ID		= 0						--基准id
local DEL_COUNT_ONE_TIME 	= 5						--一次删除redis的条目数
local CMD = {}
local gate_addr
local action_handler = {}
--------------------------------------------------------------
--函数类型	: 响应回复
--参数		:
--返回值	:
--------------------------------------------------------------
local function response(id, ...)
	local ok, err = httpd.write_response(sockethelper.writefunc(id), ...)
	if not ok then
		-- if err == sockethelper.socket_error , that means socket closed.
		skynet.error(string.format("fd = %d, %s", id, err))
	end
end
--------------------------------------------------------------
--函数类型	: 字符串id转成数字id
--参数	  :
--返回值	 :
--------------------------------------------------------------
function make_id_str2num(id_str)
	local i,j = string.find(id_str,ID_HEAD)
	return string.sub(id_str,j+1,string.len(id_str))
end
--------------------------------------------------------------
--函数类型	: 处理post请求
--参数	  :
--返回值	 :
--------------------------------------------------------------
function action_handler.POST(query,path,body,conn_id,code)
	local ret = {}
	--local path_table = string_util.split(path,"/")
	print("==notice.action_handler.POST==")
	local tab_body = JSON:decode(body)
	if body then
		if tab_body.end_time then
			local id = get_notice_id_from_redis()
			local rt = set_time_id_redis(tab_body.end_time,id)
			rt = set_id_content_redis(id,body)

			local id_num = tonumber(make_id_str2num(id))--id_num
			local tab_rsp = {noteId = id_num , errorCode = 0, errorMsg = "ok"}
			print("json_str:",tab_body)
			local json_str = JSON:encode(tab_rsp)
			if rt == 1 or rt == 0 then
				response(conn_id,code,json_str)		
			end
			--有更新,则发送给GS
			local version = get_new_version_from_redis()
			local event_array = make_event_array(0,id_num,version,body)--(type,id,version,content)
			send_notice_event_notify(req_info,event_array)
		else
			local tab_rsp = {noteId = "",errorCode = 1, errorMsg = "end_time is nil"}
			local json_str = JSON:encode(tab_rsp)
			response(conn_id,code,json_str)	
			print("action_handle.POST endTime is nil")
		end
	else
			local tab_rsp = {noteId = "",errorCode = 1, errorMsg = "body is nil"}
			local json_str = JSON:encode(tab_rsp)
			response(conn_id,code,json_str)	
			print("action_handle.POST body is nil")
	end
	
	return 0
end
--------------------------------------------------------------
--函数类型	: 处理delete请求
--参数	  :
--返回值	 :
--------------------------------------------------------------
function action_handler.DELETE(query,path,body,conn_id,code)
	local ret = {}
	print("notice.action_handler.DELETE")
--[[--]]
	--HANDLE.delete 
	local tab_body = JSON:decode(body)
	print("nodeId:",tab_body.noteId)
	if body then
		if tab_body.noteId then
			local rt = rm_time_id_redis(ID_HEAD..tab_body.noteId)
			rt = rm_id_content_redis(ID_HEAD..tab_body.noteId)

			local tab_rsp = {noteId = "",errorCode = 0, errorMsg = "ok"}
			local json_str = JSON:encode(tab_rsp)
			response(conn_id,code,json_str)	

			--有删除,则发给GS
			local version = get_new_version_from_redis()
			local event_array = make_event_array(1,tab_body.noteId,version,"")--(type,id,version,content)
			send_notice_event_notify(req_info,event_array)	
		else
			local tab_rsp = {noteId = "",errorCode = 1, errorMsg = "nodeId is nil"}
			local json_str = JSON:encode(tab_rsp)
			response(conn_id,code,json_str)	
			print("action_handle.DELETE nodeId is nil")
		end
	else
		local tab_rsp = {noteId = "",errorCode = 1, errorMsg = "body is nil"}
		local json_str = JSON:encode(tab_rsp)
		response(conn_id,code,json_str)	
		print("action_handle.DELETE body is nil")
	end
	return 0

end
--------------------------------------------------------------
--------------------------------------------------------------
--------------------------------------------------------------
--函数类型	: 发送相应消息
--参数			:
--返回值		:
--------------------------------------------------------------
function send_rsp(req_info,req,result,ret)
	local tab = {id = 11,content = "test.."}
	local tab_array = {}
	table.insert(tab_array,{id = 11,content = "test1"})
	table.insert(tab_array,{id = 12,content = "test2"})
	local rsp = {version = 22,notice_array = tab_array}

	local rsp_msg = pb.encode("gs_noteToProxyMsg.msg_notice_getall_rsp",rsp)
	skynet.send(req_info.angent,"lua","send_ny",rsp_msg,req_info.msg_id,req_info.conn_id,req_info.fd)
end
--------------------------------------------------------------
--函数类型	: 发送msg_notice_getall_rsp
--参数	  :
--返回值	 :
--------------------------------------------------------------
function send_rsp_getall_rsp(req_info,version,notice_array)
	local rsp = {}
	rsp.version = version
	rsp.notice_array = notice_array

	local rsp_msg = pb.encode("gs_noteToProxyMsg.msg_notice_getall_rsp",rsp)
	skynet.send(req_info.angent,"lua","send_ny",rsp_msg,req_info.msg_id,req_info.conn_id,req_info.fd)
end
--------------------------------------------------------------
--函数类型	: 发送msg_notice_event_notify
--参数	  :
--返回值	 :
--------------------------------------------------------------
function send_notice_event_notify(req_info,event_array)
	local event_notify = {}
	event_notify.event_array = event_array
	--print("send event_notify")
	local rsp_msg = pb.encode("gs_noteToProxyMsg.msg_notice_event_notify",event_notify)
	--skynet.send(req_info.angent,"lua","send_ny",rsp_msg,7003,req_info.conn_id,req_info.fd)
	send_to_all(rsp_msg,8003)
end
--------------------------------------------------------------
--函数类型	: 发送给所有的game_server
--参数	  :
--返回值	 :
--------------------------------------------------------------
function send_to_all(msg,msg_id)
	skynet.send(gate_addr,"lua","send_all",msg,msg_id)
end
--------------------------------------------------------------
--函数类型	: 消息分发处理
--参数			: 
--返回值		:
--------------------------------------------------------------
function CMD.on_net_message(source,angent,msg,size,msg_id,conn_id,fd)	
	local req = pb.decode("gs_noteToProxyMsg.msg_notice_getall_req",msg,size)	
	local result = 0
	--[[
	print("------heartbeat recv req---------")
	print("req.range_start:",req.range_start)
	print("req.range_end:",req.range_end)
	print("------recv req end---------")
	--debug_trace.print_r(req.content)
	--]]
	local req_info = {}
	req_info.msg_id = msg_id + 1
	req_info.conn_id = conn_id
	req_info.angent = angent
	req_info.fd = fd
	
	--gs请求拉取全量,发送当前时间所有未过期的公告
	local test_tab = get_notice_array(os.time())
	local version = get_cur_version_from_redis()
	send_rsp_getall_rsp(req_info,version,test_tab)
	--[[
	--send_req(req_info,req,"",0)
	local event_array = make_event_array(0,55,22,"test content")
	send_notice_event_notify(req_info,event_array)

	local version = get_cur_version_from_redis()
	print("version:",version)
	--]]

	--[[
	for k,v in pairs(test_tab) do
		--print("k:type(v):",k..","..type(v))
		for k1,v1 in pairs(v) do
			--print("k1,v1:",k1..","..v1)
		end
	end
	--]]
end
--------------------------------------------------------------
--------------------------------------------------------------
--------------------------------------------------------------
--函数类型	: 生成需要发送的event_array
--参数	  : 
--返回值	 : 
--------------------------------------------------------------
function make_event_array(type,id,version,content)
	local event_tab = make_notice_event(type,id,version,content)
	local event_array = {}
	table.insert( event_array, event_tab)

	return event_array
end
--------------------------------------------------------------
--函数类型	: 生成notice_event
--参数	  : 
--返回值	 : 
--------------------------------------------------------------
function make_notice_event(type,id,version,content)
	local tab_ret = {}
	tab_ret.type = type
	tab_ret.id = id
	tab_ret.version = version
	tab_ret.content = content

	return tab_ret
end
--------------------------------------------------------------
--函数类型	: 获得notice_array,当前时间所有未过期的公告
--参数	  : 
--返回值	 : {{id = 1,content = "content1"},{id = 2 , content = "content2"}}
--------------------------------------------------------------
function get_notice_array(curr_time)
	local tab = get_normal_time_id(curr_time)
	local tab_inner = {}
	local tab_ret = {}
	if next(tab) ~= nil then
		for k,v in pairs(tab) do
			if k%2 == 1 then
				local content = get_normal_id_content(v)
				tab_inner["id"] = make_id_str2num(v)
				tab_inner["content"] = content[1]
				table.insert( tab_ret, tab_inner)
				tab_inner = {}
			end
		end
	else
		print("get_all_normal_id_content:tab is nil")
	end

	return tab_ret 
end
--------------------------------------------------------------
--函数类型	: 查看过时公告数量
--参数	  : 当前时间的秒数
--返回值	 : 返回过时公告数目
--------------------------------------------------------------
function check_out_time_note_count(curr_time)
	local cmd = "ZCOUNT"
	local param = NOTICE_ID_KEY.." ".."0".." "..curr_time
	local ret = redis_db[cmd](redis_db,string_util.split(param," "))
	return ret 
end
--------------------------------------------------------------
--函数类型	: 返回已经过期的成员
--参数	  :
--返回值	 : {"id1","id2",...}
--------------------------------------------------------------
function get_out_time_id(curr_time)--ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
	local cmd = "ZRANGEBYSCORE"
	local param = NOTICE_ID_KEY.." ".."0".." "..curr_time.." ".."WITHSCORES"
	local ret = redis_db[cmd](redis_db,string_util.split(param," "))

	return ret
end
--------------------------------------------------------------
--函数类型	: 返回 没有 过期的成员
--参数	  :
--返回值	 : {"id1","id2",...}
--------------------------------------------------------------
function get_normal_time_id(curr_time)--ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
	local cmd = "ZRANGEBYSCORE" 
	--ZRANGEBYSCORE xgameNoteIdKey 1508997422 2489735820 WITHSCORES
	local ret = redis_db[cmd](redis_db,NOTICE_ID_KEY,curr_time,"2489735820","WITHSCORES")

	return ret
end
--------------------------------------------------------------
--函数类型	: 返回id:content
--参数	  :
--返回值	 : {"id1"=content1,"id2"=content2,...}
--------------------------------------------------------------
function get_normal_id_content(id)--SMEMBERS KEY VALUE
	local cmd = "SMEMBERS"
	local ret = redis_db[cmd](redis_db,id)

	return ret
end
--------------------------------------------------------------
--函数类型	: 移除time:id中已经过期的time:id
--参数	  :
--返回值	 : 
--------------------------------------------------------------
function rm_out_time_id(curr_time)--ZREMRANGEBYSCORE key min max
	local cmd = "ZREMRANGEBYSCORE"
	local param = NOTICE_ID_KEY.." ".."0".." "..curr_time
	local ret = redis_db[cmd](redis_db,string_util.split(param," "))

	return ret
end
--------------------------------------------------------------
--函数类型	: 从time:id获取前n条的id
--参数	  :
--返回值	 :
--------------------------------------------------------------
function get_id_by_count(count)-- ZRANGE key start stop [WITHSCORES]
	local cmd = "ZRANGE"
	local ret = redis_db[cmd](redis_db,NOTICE_ID_KEY,0,count-1)

	return ret
end
--------------------------------------------------------------
--函数类型	: 删除前 n 条time:id
--参数	  :
--返回值	 :
--------------------------------------------------------------
function del_time_id_by_count(count)--ZREMRANGEBYRANK key start stop
	local cmd = "ZREMRANGEBYRANK"
	local ret = redis_db[cmd](redis_db,NOTICE_ID_KEY,0,count-1)
end
--------------------------------------------------------------
--函数类型	: 删除前 n 条公告id:content
--参数	  :
--返回值	 :
--------------------------------------------------------------
function del_note_by_count(count)-----未完成	
	local tab = get_id_by_count(count)
	if tab then
		for k,v in pairs(tab) do  
			rm_id_content_redis(v)
		end
	end
	del_time_id_by_count(count)
end
--------------------------------------------------------------
--函数类型	: 删除已经过时的公告
--参数	  :
--返回值	 :
--------------------------------------------------------------
function del_out_time_note(curr_time)
	local tab = get_out_time_id(curr_time)
	if next(tab) ~= nil then --非空表,则有过时公告
		rm_out_time_id(curr_time)
		for k,v in pairs(tab) do
			if k%2 == 1 then
				rm_id_content_redis(v)
			end
		end
	end 
end
--------------------------------------------------------------
--函数类型	: 检查公告是否过期等
--参数	  :
--返回值	 :
--------------------------------------------------------------
function check()
	--todo
	local count = 0
	if redis_db ~= nil then 
		local count_tmp = check_out_time_note_count(os.time())
		count = count + count_tmp
		if count > 0 then
			if count > DEL_COUNT_ONE_TIME then
				del_note_by_count(DEL_COUNT_ONE_TIME)
				count = count - DEL_COUNT_ONE_TIME
			else
				del_note_by_count(count)
			end
		end
	else
		print("waitting redis_db..")
	end
end

function handler_mainloop()
	while true do
		--check timeout
		check()

		skynet.sleep(100)  --1 second
	end
end
--------------------------------------------------------------
--函数类型	: 获取当前版本号
--参数	  :
--返回值	 : 新的版本号
--------------------------------------------------------------
function get_cur_version_from_redis()
	local cmd = "GET"
	local version = redis_db[cmd](redis_db,NOTICE_VERSION_KEY)
		
	return version
end
--------------------------------------------------------------
--函数类型	: 获取新的版本号
--参数	  :
--返回值	 : 新的版本号
--------------------------------------------------------------
function get_new_version_from_redis()
	local cmd = "INCR"
	local version = redis_db[cmd](redis_db,NOTICE_VERSION_KEY)
		
	return version
end
--------------------------------------------------------------
--函数类型	: 从redis获取id
--参数	  :
--返回值	 : 增加的下一个id
--------------------------------------------------------------
function get_notice_id_from_redis()
	local cmd = "INCR"
	NOTICE_BASE_ID = redis_db[cmd](redis_db,NOTICE_REDIS_ID)
		
	return ID_HEAD..NOTICE_BASE_ID
end
--------------------------------------------------------------
--函数类型	: 把超时time和id以zset方式写入redis
--参数	  :
--返回值	 :
--------------------------------------------------------------
function set_time_id_redis(end_time,id)
	local cmd = "ZADD"
	local param = NOTICE_ID_KEY.." "..end_time.." "..id
	local ret = redis_db[cmd](redis_db,string_util.split(param," "))
	return ret
end
--------------------------------------------------------------
--函数类型	: 把超时id和content以set方式写入redis
--参数	  :
--返回值	 :
--------------------------------------------------------------
function set_id_content_redis(id,content)
	local cmd = "SADD"
	local param = id.." "..content
	local ret = redis_db[cmd](redis_db,string_util.split(param," "))
	return ret
end
--------------------------------------------------------------
--函数类型	: 删除end_time:id 的zset
--参数	  :
--返回值	 :
--------------------------------------------------------------
function rm_time_id_redis(id)
	local cmd = "zrem"
	local param = NOTICE_ID_KEY.." "..id 
	local ret = redis_db[cmd](redis_db,string_util.split(param," "))
end
--------------------------------------------------------------
--函数类型	: 删除id:content 的set
--参数	  :
--返回值	 :
--------------------------------------------------------------
function rm_id_content_redis(id)
	local cmd = "spop"
	local param = id
	local ret = redis_db[cmd](redis_db,string_util.split(param," "))
end
--------------------------------------------------------------
function CMD.request(source,code,path,query,method,header,body,conn_id)
	local tmp = {}
	local ret_code = 200
	
	local action = method
	local f = action_handler[action]
	if f then
		ret_code,tmp = f(query,path,body,conn_id,code)
	else
		table.insert(tmp, "action not find!")
		return 404,tmp
	end
	return ret_code,tmp
end
--------------------------------------------------------------
--函数类型	: 初始化reids
--参数	  :
--返回值	 :
--------------------------------------------------------------
function CMD.init_redis(source,reids_conf,gate)
	print("init redis")
	gate_addr = gate
	redis_db = redis.connect(reids_conf)
	if redis_db == nil then 
		print("redis_db is nil ")
	else
		print("redis_db is ready")
	end
end

skynet.start(function()
	pb.register_file("dbs_demo/proto/gs_note2proxy")	
	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)
	skynet.fork(handler_mainloop)	
end)
