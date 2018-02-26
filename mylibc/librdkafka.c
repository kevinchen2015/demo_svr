#ifdef __cplusplus

extern "C" {
#endif

#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
#ifdef __cplusplus
}
#endif

#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "skynet_malloc.h"

#include <dlfcn.h>
#include <stdio.h>

#include <librdkafka/rdkafka.h>
#include "uthash.h"

enum cb_type
{
	delivery_cb = 0,
	stat_cb,
	error_cb,
	log_cb,

	max_cb,
};

//node
struct node_t {

	rd_kafka_t* rk;
	rd_kafka_conf_t* conf;
	lua_State* L;
	UT_hash_handle hh;
	int cb[max_cb];
};

struct node_t* g_data_root;

static struct node_t* node_find(rd_kafka_conf_t* conf)
{
	struct node_t *t, *tmp;
	HASH_ITER(hh, g_data_root, t, tmp) {
		if(t->conf == conf){
			return t;
		}
	}
	return (struct node_t*)0;
}

static struct node_t* node_find_by_rk(rd_kafka_t* rk)
{
	struct node_t* t = (struct node_t *)0;
	struct node_t* tmp = t;
	//HASH_FIND_PTR(g_data_root,&rk,t); 
	HASH_ITER(hh,g_data_root,t,tmp){
		if(t->rk == rk)
			return t;
	}
	return t;
}

static struct node_t* node_create(lua_State* L,rd_kafka_conf_t* conf,rd_kafka_t* rk)
{
	struct node_t* p = (struct node_t*)skynet_malloc(sizeof(struct node_t));
	p->conf = conf;
	p->L = L;
	p->rk = rk;
	for(int i = 0;i<max_cb;++i)
	{
		p->cb[i] = 0;
	}
	return p;
}

static void node_add(struct node_t* node)
{
	HASH_ADD_PTR(g_data_root,rk,node);
}

static void node_remove(struct node_t* node)
{
	struct node_t* t = (struct node_t *)0;
	HASH_FIND_PTR(g_data_root,node->rk, t);
	if (t) {
		HASH_DEL(g_data_root, t);
		return;
	}
}

static void node_destory(struct node_t* node)
{
	for(int i = 0;i<max_cb;++i)
	{
		if(node->cb[i] != 0)
		{
			luaL_unref(node->L, LUA_REGISTRYINDEX, node->cb[i]);
		}
	}
	skynet_free(node);
}

//--------------------------------------------------------------------
void 
on_delivery(rd_kafka_t *rk,  
    const rd_kafka_message_t *rkmessage, void *opaque)
{
	struct node_t* node = node_find_by_rk(rk);
	if(node) 
	{
		lua_State* L  = node->L;
		lua_rawgeti(L, LUA_REGISTRYINDEX, node->cb[delivery_cb]);
		lua_pushlightuserdata(L,rk);
		lua_pushinteger(L,(int)rkmessage->err);
		lua_pushlightuserdata(L,rkmessage->rkt);
		lua_pushinteger(L,rkmessage->partition);
		lua_pushinteger(L,rkmessage->offset);
		lua_pushinteger(L,rkmessage->len);
		//lua_pushlstring(L,(const char*)payload,len);
		//lua_pushlightuserdata(L,opaque);
		lua_call(L, 6, 0);
	}
}

int 
on_stats (rd_kafka_t *rk,
	char *json,
	size_t json_len,
	void *opaque)
{
	struct node_t* node = node_find_by_rk(rk);
	if(node) 
	{
		lua_State* L  = node->L;
		lua_rawgeti(L, LUA_REGISTRYINDEX, node->cb[stat_cb]);
		lua_pushlightuserdata(L,rk);
		lua_pushstring(L,(const char*)json);
		lua_pushinteger(L,json_len);
		lua_pushlightuserdata(L,opaque);
		lua_call(L, 4, 0);
	}
	return 0;
}

void 
 on_error (rd_kafka_t *rk, int err,
	const char *reason,
	void *opaque)
{
	struct node_t* node = node_find_by_rk(rk);
	if(node) 
	{
		lua_State* L  = node->L;
		lua_rawgeti(L, LUA_REGISTRYINDEX, node->cb[error_cb]);
		lua_pushlightuserdata(L,rk);
		lua_pushinteger(L,err);
		lua_pushstring(L,(const char*)reason);
		lua_call(L, 3, 0);
	}
}

void 
on_log (const rd_kafka_t *rk, int level,
	const char *fac, const char *buf)
{
	struct node_t* node = node_find_by_rk(rk);
	if(node) 
	{
		lua_State* L  = node->L;
		lua_rawgeti(L, LUA_REGISTRYINDEX, node->cb[log_cb]);
		lua_pushlightuserdata(L,rk);
		lua_pushinteger(L,level);
		lua_pushstring(L,(const char*)fac);
		lua_pushstring(L,(const char*)buf);
		lua_call(L, 4, 0);
	}
}

//-----------------------------------------------------------------

static int
l_rd_kafka_null_func(lua_State* L) {
	lua_pushnil(L);
	return 1;
}

static int
l_rd_kafka_errno2err(lua_State* L) {
	int errnox = lua_tointeger(L,1);
	int ret = rd_kafka_errno2err(errnox);
	lua_pushinteger(L,ret);
	return 1;
}

//----------------------------------------------------------------

static int
l_rd_kafka_conf_new(lua_State* L) {
	rd_kafka_conf_t* conf = rd_kafka_conf_new();
	lua_pushlightuserdata(L,conf);

	struct node_t* node = node_find(conf);
	if(!node)
	{
		node = node_create(L,conf,(rd_kafka_t*)0);
	}
	if(node)
	{
		node_add(node);
	}
	return 1;
}

static int
l_rd_kafka_conf_dup(lua_State* L) {
	const rd_kafka_conf_t* conf = (const rd_kafka_conf_t*)lua_touserdata(L,1);
	rd_kafka_conf_t* new_conf = rd_kafka_conf_dup(conf);
	lua_pushlightuserdata(L,new_conf);
	return 1;
}

static int
l_rd_kafka_conf_destroy(lua_State* L) {
	rd_kafka_conf_t* conf = (rd_kafka_conf_t*)lua_touserdata(L,1);
	rd_kafka_conf_destroy(conf);

	struct node_t* node = node_find(conf);
	if(node)
	{
		node_remove(node);
		node_destory(node);
	}
	return 0;
}

static int
l_rd_kafka_conf_set(lua_State* L) {
	rd_kafka_conf_t* conf = (rd_kafka_conf_t*)lua_touserdata(L,1);
	const char* name = lua_tostring(L,2);
	const char* value = lua_tostring(L,3);
	size_t size = lua_tointeger(L,5);
	char* temp = (char*)skynet_malloc(size); 
	rd_kafka_conf_res_t ret = rd_kafka_conf_set(conf,name,value,temp,size);
	lua_pushinteger(L,ret);
	lua_pushstring(L,temp);
	skynet_free(temp);
	return 2;
}

static int
l_rd_kafka_conf_set_cb(lua_State* L) {
	rd_kafka_conf_t* conf = (rd_kafka_conf_t*)lua_touserdata(L,1);
	int type = lua_tointeger(L,2);
	if(type >= max_cb || type < 0)
	{
		lua_tointeger(L,1);
		return 1;
	}
	struct node_t* node = node_find(conf);
	if(node)
	{
		node->cb[type] = luaL_ref(L, LUA_REGISTRYINDEX);
		switch(type)
		{
			case delivery_cb:
				rd_kafka_conf_set_dr_msg_cb(conf,on_delivery);
			break;

			case stat_cb:
				rd_kafka_conf_set_stats_cb(conf,on_stats);
			break;

			case error_cb:
				rd_kafka_conf_set_error_cb(conf,on_error);
			break;	

			case log_cb:
				rd_kafka_conf_set_log_cb(conf,on_log);
			break;


		}
	}
	else
	{
		lua_tointeger(L,2);
		return 1;
	}
	lua_tointeger(L,0);
	return 1;
}

//----------------------------------------------------------------------

static int
l_rd_kafka_new(lua_State* L) {
	int type = lua_tointeger(L,1);
	rd_kafka_conf_t* conf = lua_touserdata(L,2);
	size_t err_len = lua_tointeger(L,4);
	char* temp = (char*)skynet_malloc(err_len); 
	rd_kafka_t* kafaka = rd_kafka_new(type,conf,temp,err_len);
	lua_pushlightuserdata(L,kafaka);
	lua_pushstring(L,temp);
	skynet_free(temp);

	struct node_t* node = node_find(conf);
	if(!node)
	{
		node = node_create(L,conf,kafaka);
	}else
	{
		if(!node->rk)
		{
			node->rk = kafaka;
		}
		else
		{
			printf("\r\n l_rd_kafka_new(),error : node->rk != NULL  \r\n");
		}
	}
	return 2;
}

static int
l_rd_kafka_destroy(lua_State* L) {
	rd_kafka_t* kafaka = (rd_kafka_t*)lua_touserdata(L,1);
	rd_kafka_destroy(kafaka);

	struct node_t* node = node_find_by_rk(kafaka);
	if(node)
	{
		node->rk = (rd_kafka_t*)0;
	}
	return 0;
}

static int
l_rd_kafka_wait_destroyed(lua_State* L) {
	int ms = lua_tointeger(L,1);
	int ret = rd_kafka_wait_destroyed(ms);
	lua_pushinteger(L,ret);
	return 1;
}

static int
l_rd_kafka_brokers_add(lua_State* L) {
	rd_kafka_t* kafaka = (rd_kafka_t*)lua_touserdata(L,1);
	const char* broker_list = lua_tostring(L,2);
	int ret = rd_kafka_brokers_add(kafaka,broker_list);
	lua_pushinteger(L,ret);
	return 1;
}

static int
l_rd_kafka_produce(lua_State* L) {
	rd_kafka_topic_t* topic = (rd_kafka_topic_t*)lua_touserdata(L,1);
	int partition = lua_tointeger(L,2);
	int msgflags = lua_tointeger(L,3);
	size_t len;
	void* payload = lua_tolstring(L,4,&len);
	size_t key_len;
	void* key = lua_tolstring(L,6,&key_len);
	int ret = rd_kafka_produce(topic,partition,msgflags,payload,len,(const void*)key,key_len,(void*)0);
	lua_pushinteger(L,ret);
	return 1;
}

static int
l_rd_kafka_poll(lua_State* L) {
	rd_kafka_t* kafaka = (rd_kafka_t*)lua_touserdata(L,1);
	int timeout_ms = lua_tointeger(L,2);
	int ret = rd_kafka_poll(kafaka,timeout_ms);
	lua_pushinteger(L,ret);
	return 1;
}

static int
l_rd_kafka_outq_len(lua_State* L) {
	rd_kafka_t* kafaka = (rd_kafka_t*)lua_touserdata(L,1);
	int ret = rd_kafka_outq_len(kafaka);
	lua_pushinteger(L,ret);
	return 1;
}

static int
l_rd_kafka_thread_cnt(lua_State* L) {
	int ret = rd_kafka_thread_cnt();
	lua_pushinteger(L,ret);
	return 1;
}

//-------------------------------------------------------------------------
static int
l_rd_kafka_consume_start(lua_State* L) {
	rd_kafka_topic_t* rkt = (rd_kafka_topic_t*)lua_touserdata(L,1);
	int partition = lua_tointeger(L,2);
	long long start_offset = (long long )lua_tonumber(L,3);
	int ret = 0;
	if(rd_kafka_consume_start(rkt,partition,start_offset) == -1){
		rd_kafka_resp_err_t err = rd_kafka_last_error();
		ret = (int)err;
	}
	lua_pushinteger(L,ret);
	return 1;
}
static int
l_rd_kafka_consume_stop(lua_State* L) {
	rd_kafka_topic_t* rkt = (rd_kafka_topic_t*)lua_touserdata(L,1);
	int partition = lua_tointeger(L,2);
	rd_kafka_consume_stop(rkt,partition);
	return 0;
}

static int
l_rd_kafka_consume(lua_State* L) {
	rd_kafka_topic_t* rkt = (rd_kafka_topic_t*)lua_touserdata(L,1);
	int partition = lua_tointeger(L,2);
	int timeout_ms = lua_tointeger(L,3);
	rd_kafka_message_t* rkmessage = rd_kafka_consume(rkt, partition, timeout_ms);
	if (rkmessage)
	{	
		//function must be stack top
		printf("\n ");
		if(!lua_isnil(L,4))
		{
			lua_pushinteger(L,(int)rkmessage->err);
			lua_pushlightuserdata(L,rkmessage->rkt);
			lua_pushinteger(L,rkmessage->partition);
			lua_pushlstring(L,(const char*)rkmessage->payload,rkmessage->len);
			lua_pushnumber(L,rkmessage->offset);
			lua_call(L, 5, 0);
		}
		rd_kafka_message_destroy(rkmessage);
	}
	return 0;
}

//-----------------------------------------------------------------------

static int
l_rd_kafka_topic_conf_new(lua_State* L) {
	rd_kafka_topic_conf_t* topic_conf = rd_kafka_topic_conf_new();
	lua_pushlightuserdata(L,topic_conf);
	return 1;
}

static int
l_rd_kafka_topic_conf_dup(lua_State* L) {
	const rd_kafka_topic_conf_t* conf = (const rd_kafka_topic_conf_t*)lua_touserdata(L,1);
	rd_kafka_topic_conf_t* new_conf = rd_kafka_topic_conf_dup(conf);
	lua_pushlightuserdata(L,new_conf);
	return 1;
}

static int
l_rd_kafka_topic_conf_destroy(lua_State* L) {
	rd_kafka_topic_conf_t* conf = (rd_kafka_topic_conf_t*)lua_touserdata(L,1);
	rd_kafka_topic_conf_destroy(conf);
	return 0;
}

static int
l_rd_kafka_topic_conf_set(lua_State* L) {
	rd_kafka_topic_conf_t* conf = (rd_kafka_topic_conf_t*)lua_touserdata(L,1);
	const char* name = lua_tostring(L,2);
	const char* value = lua_tostring(L,3);
	size_t size = lua_tointeger(L,5);
	char* temp = (char*)skynet_malloc(size); 
	rd_kafka_conf_res_t ret = rd_kafka_topic_conf_set(conf,name,value,temp,size);
	lua_pushinteger(L,ret);
	lua_pushstring(L,temp);
	skynet_free(temp);
	return 2;
}

//----------------------------------------------------------------------------
static int
l_rd_kafka_topic_new(lua_State* L) {
	rd_kafka_t* kafka = (rd_kafka_t*)lua_touserdata(L,1);
	const char* name = lua_tostring(L,2);
	rd_kafka_topic_conf_t* conf = (rd_kafka_topic_conf_t*)lua_touserdata(L,3);
	rd_kafka_topic_t* topic = rd_kafka_topic_new(kafka,name,conf);
	lua_pushlightuserdata(L,topic);
	return 1;
}

static int
l_rd_kafka_topic_name(lua_State* L) {
	const rd_kafka_topic_t* topic = (const rd_kafka_topic_t*)lua_touserdata(L,1);
	const char* name = rd_kafka_topic_name(topic);
	lua_pushlightuserdata(L,name);
	return 1;
}

//----------------------------------------------------------------------------

int
luaopen_rdkafka(lua_State *L) {

	g_data_root = (struct node_t*)0;

	luaL_Reg reg[] = {
		{"rd_kafka_errno2err",l_rd_kafka_errno2err},
		//config
		{"rd_kafka_conf_new",  l_rd_kafka_conf_new },
		{"rd_kafka_conf_dup",  l_rd_kafka_conf_dup },
		{"rd_kafka_conf_destroy",  l_rd_kafka_conf_destroy },
		{"rd_kafka_conf_set", l_rd_kafka_conf_set  },
		{"rd_kafka_conf_set_cb",  l_rd_kafka_conf_set_cb }, 
		//producer 
		{"rd_kafka_new", l_rd_kafka_new},
		{"rd_kafka_destroy",  l_rd_kafka_destroy },
		{"rd_kafka_wait_destroyed",  l_rd_kafka_wait_destroyed },
		{"rd_kafka_brokers_add", l_rd_kafka_brokers_add  },
		{"rd_kafka_produce",  l_rd_kafka_produce },
		{"rd_kafka_poll",  l_rd_kafka_poll },
		{"rd_kafka_outq_len",  l_rd_kafka_outq_len },
		{"rd_kafka_thread_cnt",  l_rd_kafka_thread_cnt },
		//counsum 
		{"rd_kafka_consume_start", l_rd_kafka_consume_start},
		{"rd_kafka_consume_stop", l_rd_kafka_consume_stop},
		{"rd_kafka_consume", l_rd_kafka_consume},
		//topic_config
		{"rd_kafka_topic_conf_new", l_rd_kafka_topic_conf_new  },
		{"rd_kafka_topic_conf_dup",  l_rd_kafka_topic_conf_dup },
		{"rd_kafka_topic_conf_destroy", l_rd_kafka_topic_conf_destroy  },
		{"rd_kafka_topic_conf_set",  l_rd_kafka_topic_conf_set },
		//topic
		{"rd_kafka_topic_new",  l_rd_kafka_topic_new },
		{"rd_kafka_topic_name", l_rd_kafka_topic_name  },

		{NULL,NULL},
	};

	luaL_checkversion(L);
	luaL_newlib(L, reg);

	return 1;
}
