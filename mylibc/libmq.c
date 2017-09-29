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


#ifndef NULL
#define NULL (void*)0
#endif

static void* _handle;

static void close()
{
	if (_handle)
	{
		dlclose(_handle);
		_handle = NULL;
	}
}

static int open(const char* so_file_path)
{
	close();

	_handle = dlopen(so_file_path, RTLD_LAZY | RTLD_GLOBAL);
	if (_handle == NULL) {
		printf("dlopen: %s\n", dlerror());
		printf("can not find so ,path: %s\n", so_file_path);
		return -1;
	}
	return 0;
}

static void* get_func(const char* func_name)
{
	dlerror();    /* Clear any existing error */
	void* ret = dlsym(_handle, func_name);

	char *error;
	if ((error = dlerror()) != NULL)
		return NULL;
	else
		return ret;
}

//-------------------------------------------------------------------

struct sharemem_t {
	void* sm_handle;
	char* sm_buff;
	unsigned int sm_buff_len;
};

typedef void(*on_mq_recv)(void* data, unsigned int len, void* user_data);
typedef int(*init)(const char* cfg_name);
typedef void(*uninit)();
typedef int(*update)();
typedef void*(*query_mq)(const char* mq_name);
typedef int(*set_callback)(void* mq_handle, on_mq_recv cb, void* user_data);
typedef struct sharemem_t(*alloc_sharemem)(unsigned int len);
typedef int(*enqueue_smdata)(void* mq_handle, struct sharemem_t* sm);

struct  libmq_funcs
{
	init init_;
	uninit uninit_;
	update update_;
	query_mq query_mq_;
	set_callback set_callback_;
	alloc_sharemem alloc_sharemem_;
	enqueue_smdata enqueue_smdata_;
	int lua_cb_;
	lua_Integer lua_cb_param_;
};

static struct libmq_funcs mq;

//----------------------------------------------------------------------------

static int
mq_init(lua_State* L) {

	const char* so = lua_tostring(L, 1);
	const char* cfg = lua_tostring(L, 2);

	memset(&mq, 0x00, sizeof(struct libmq_funcs));
	mq.lua_cb_ = LUA_REFNIL;

	int ret = open(so);
	if (ret != 0)
	{
		printf("\r\n load libmq.so failed!!\r\n");
		lua_pushinteger(L, ret);
		return 1;
	}

	mq.init_ = (init)get_func("mq_init");
	mq.uninit_ = (uninit)get_func("mq_uninit");
	mq.update_ = (update)get_func("mq_update");
	mq.query_mq_ = (query_mq)get_func("mq_query_by_name");
	mq.set_callback_ = (set_callback)get_func("mq_set_callback");
	mq.alloc_sharemem_ = (alloc_sharemem)get_func("mq_alloc_sharemem");
	mq.enqueue_smdata_ = (enqueue_smdata)get_func("mq_enqueue_smdata");

	ret = mq.init_(cfg);
	lua_pushinteger(L, ret);
	return 1;
}

static int
mq_uninit(lua_State* L) {

	luaL_unref(L, LUA_REGISTRYINDEX, mq.lua_cb_);
	mq.lua_cb_ = LUA_REFNIL;

	mq.uninit_();
	return 0;
}


static int
mq_update(lua_State* L) {
	int ret = mq.update_();
	lua_pushinteger(L, ret);
	return 1;
}

static int
mq_query_by_name(lua_State* L) {
	const char* name = lua_tostring(L, 1);
	void* handle = mq.query_mq_(name);
	lua_pushlightuserdata(L, handle);
	return 1;
}

void on_recv_(void* data, unsigned int len, void* user_data)
{
	lua_State* L = (lua_State*)user_data;
	lua_rawgeti(L, LUA_REGISTRYINDEX, mq.lua_cb_);
	lua_pushlstring(L,(const char*)data,len);
	lua_pushinteger(L, len);
	lua_pushinteger(L, mq.lua_cb_param_);
	lua_call(L, 3, 1);
}

static int
mq_set_callback(lua_State* L) {
	void* mq_handle = lua_topointer(L, 1);
	mq.lua_cb_param_ = lua_tointeger(L, 3);
	lua_pop(L, 1);
	mq.lua_cb_ = luaL_ref(L, LUA_REGISTRYINDEX);
	
	int ret = mq.set_callback_(mq_handle, on_recv_,L);
	lua_pushinteger(L, ret);
	return 1;
}

static int
mq_enqueue_smdata(lua_State* L) {
	void * mq_handle = lua_topointer(L, 1);
	size_t msg_len = 0;
	const char* msg = lua_tolstring(L, 2,&msg_len);
	struct sharemem_t sm = mq.alloc_sharemem_(msg_len);
	memcpy(sm.sm_buff, msg, msg_len);
	int ret = mq.enqueue_smdata_(mq_handle, &sm);
	lua_pushinteger(L, ret);
	return 1;
}

//----------------------------------------------------------------------------

int
luaopen_smq(lua_State *L) {

	luaL_Reg reg[] = {
		{"mq_init",  mq_init },
		{"mq_uninit",  mq_uninit },
		{"mq_update", mq_update },
		{"mq_query_by_name", mq_query_by_name },
		{"mq_set_callback", mq_set_callback },
		//{"mq_alloc_sharemem", mq_alloc_sharemem },
		{"mq_enqueue_smdata",mq_enqueue_smdata },
		{NULL,NULL},
	};

	luaL_checkversion(L);
	luaL_newlib(L, reg);

	return 1;
}
