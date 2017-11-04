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


#include <dlfcn.h>
#include <stdio.h>
#include <pthread.h>

#include <zookeeper.h>
#include <zookeeper_log.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>

#include "skynet_malloc.h"
#include "znode_high.h"
#include "smemory.h"

static int gs_data_event_cb;
static int gs_async_error_cb;
static lua_State* gs_L;		//global only one!

static void 
on_data_event(enum znode_high_event event, struct znode_high_data_* data) {
	if (gs_data_event_cb) {
		lua_State* L = gs_L;
		lua_rawgeti(L, LUA_REGISTRYINDEX, gs_data_event_cb);
		lua_pushinteger(L, (int)event);
		if (data->path)
			lua_pushstring(L, data->path);
		else
			lua_pushnil(L);
		lua_pushinteger(L, data->version);
		lua_pushlstring(L, data->value, data->value_len);
		lua_call(L, 4, 0);
	}
}

static void 
on_async_error(char* path, int op_type, int ret) {
	if (gs_async_error_cb) {
		lua_State* L = gs_L;
		lua_rawgeti(L, LUA_REGISTRYINDEX, gs_async_error_cb);
		if (path)
			lua_pushstring(L, path);
		else
			lua_pushnil(L);
		lua_pushinteger(L, op_type);
		lua_pushinteger(L, ret);
		lua_call(L, 3, 0);
	}
}

static int
lznode_set_debug_level(lua_State* L) {
	int level = lua_tointeger(L,1);
	znode_high_set_debug_level(level);
	return 0;
}

static int
lznode_init(lua_State* L) {
	gs_data_event_cb = 0;
	gs_async_error_cb = 0;
	const char* host = lua_tostring(L,1);
	int timeout = lua_tointeger(L,2);
	struct znode_high_callback cb;
	cb.event_cb = on_data_event;
	cb.error_cb = on_async_error;
	int ret = znode_high_init(host, timeout, &cb);
	lua_pushinteger(L, ret);
	return 1;
}

static int
lznode_set_data_event_cb(lua_State* L) {
	gs_data_event_cb = luaL_ref(L, LUA_REGISTRYINDEX);
	return 0;
}

static int
lznode_set_async_error_cb(lua_State* L) {
	gs_async_error_cb = luaL_ref(L, LUA_REGISTRYINDEX);
	return 0;
}

static int
lznode_uninit(lua_State* L) {
	znode_high_uninit();
	return 0;
}

static int 
lznode_update(lua_State* L) {
	znode_high_update();
	return 0;
}

static int
lznode_add_watch_path(lua_State* L) {
	const char* path = lua_tostring(L, 1);
	int is_watch_child = lua_tointeger(L, 2);
	znode_high_watch_path(path, is_watch_child);
	return 1;
}

static int
lznode_remove_watch_path(lua_State* L) {
	const char* path = lua_tostring(L, 1);
	znode_high_remove_watch_path(path);
	return 0;
}

//同步-----------------------
static int 
lznode_create(lua_State* L) {
	size_t path_len;
	const char* path = lua_tolstring(L,1,&path_len);
	size_t len;
	const char* value = lua_tolstring(L,2,&len);
	int flag = lua_tointeger(L,3);
	int rc = znode_high_create(path, value, len, flag);
	lua_pushinteger(L,rc);
	return 1;
}

static int 
lznode_exists(lua_State* L) {
	const char* path = lua_tostring(L,1);
	int ret = znode_high_exists(path);
	lua_pushinteger(L,ret);
	return 1;
}

static int 
lznode_delete(lua_State* L) {
	const char* path = lua_tostring(L,1);
	int version = lua_tointeger(L,2);
	int ret = znode_high_delete(path,version);
	lua_pushinteger(L,ret);
	return 1;
}

static int 
lznode_get(lua_State* L) {
	const char* path = lua_tostring(L,1);
	char buffer[1024] = {0x00};
	int len = 1024;
	int version = 0;
	int ret = znode_high_get(path,buffer,&len,&version);
	lua_pushinteger(L,ret);
	lua_pushlstring(L,buffer,len);
	lua_pushinteger(L,version);
	return 3;
}

static int
lznode_set(lua_State* L) {
	const char* path = lua_tostring(L,1);
	int len;
	const char* buffer = lua_tolstring(L,2,&len);
	int version = lua_tointeger(L,4);
	int ret = znode_high_set(path,buffer,len,version);
	lua_pushinteger(L,ret);
	return 1;
}

static int
lznode_get_children(lua_State* L) {
	const char* path = lua_tostring(L,1);
	int count;
	char** child_path;
	int version = 0;
	int ret = znode_high_get_children(path,&count, child_path,&version);
	lua_pushinteger(L,ret);
	lua_pushinteger(L, version);
	lua_newtable(L);
	for(int i=0;i<count;++i)
	{
		lua_pushstring(L, child_path[i]);
		lua_seti(L,-2,i+1);
	}
	return 3;
}

//异步-------------------------------------------------------------------------------

static int 
lznode_acreate(lua_State* L) {
	size_t path_len;
	const char* path = lua_tostring(L,1);
	size_t len;
	const char* value = lua_tolstring(L,2,&len);
	int flag = lua_tointeger(L,3);
	int ret = znode_high_acreate(path, value, len, flag);
	lua_pushinteger(L, ret);
	return 1;
}

static int 
lznode_adelete(lua_State* L) {
	const char* path = lua_tostring(L,1);
	int version = lua_tointeger(L,2);
	int ret = znode_high_adelete(path,version);
	lua_pushinteger(L,ret);
	return 1;
}

static int 
lznode_aexists(lua_State* L) {
	const char* path = lua_tostring(L,1);
	int ret = znode_high_aexists(path);
	lua_pushinteger(L,ret);
	return 1;
}

static int 
lznode_aget(lua_State* L) {
	const char* path = lua_tostring(L,1);
	int ret = znode_high_aget(path);
	lua_pushinteger(L,ret);
	return 1;
}

static int
lznode_aset(lua_State* L) {
	const char* path = lua_tostring(L,1);
	int len;
	const char* buffer = lua_tolstring(L,2,&len);
	int version = lua_tointeger(L,3);
	int ret = znode_high_aset(path,buffer,len,version);
	lua_pushinteger(L,ret);
	return 1;
}

static int
lznode_aget_children(lua_State* L) {
	const char* path = lua_tostring(L,1);
	int ret = znode_high_aget_children(path);
	lua_pushinteger(L,ret);
	return 1;
}


static int
lsmem_debug_enable(lua_State* L) {
	int enable = lua_tointeger(L, 1);
	smem_debug_enable(enable);
	return 0;
}

static int
lsmem_debug_print(lua_State* L) {
	smem_debug_print();
	return 0;
}


int
luaopen_znode(lua_State *L) {
	gs_L = L;
	
	luaL_Reg reg[] = {
		{"smem_debug_enable",lsmem_debug_enable },
		{"smem_debug_print",lsmem_debug_print },

		{"znode_set_debug_level",lznode_set_debug_level},
		{"znode_init",lznode_init},
	
		{"znode_set_data_event_cb",lznode_set_data_event_cb},
		{"znode_set_async_error_cb",lznode_set_async_error_cb},

		{"znode_uninit",lznode_uninit },
		{"znode_update",lznode_update},

		{ "znode_add_watch_path",lznode_add_watch_path },
		{ "znode_remove_watch_path",lznode_remove_watch_path },

		//同步接口,注意使用场景
		{"znode_create",lznode_create},
		{"znode_delete",lznode_delete},
		{"znode_exists",lznode_exists},
		{"znode_get",lznode_get},
		{"znode_set",lznode_set},
		{"znode_get_children",lznode_get_children},

		//异步接口,主要使用
		{"znode_acreate",lznode_acreate},
		{"znode_adelete",lznode_adelete},
		{"znode_aexists",lznode_aexists},
		{"znode_aget",lznode_aget},
		{"znode_aset",lznode_aset},
		{"znode_aget_children",lznode_aget_children},

		{NULL,NULL},
	};

	luaL_checkversion(L);
	luaL_newlib(L, reg);
	return 1;
}
