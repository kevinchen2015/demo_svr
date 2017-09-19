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

#define PACKAGE_HEAD_SIZE 8
/*
typedef struct{
	unsigned short des_type;
	unsigned short des_id;
	unsigned short src_type;
	unsigned short src_id;
} package_head;
*/
unsigned short exchange_byte(unsigned short v){
	return (unsigned short)((v << 8) | (v >> 8));
}

#define NET_TO_HOST_UINT16 exchange_byte
#define HOST_TO_NET_UINT16 exchange_byte

static const void *
getbuffer(lua_State *L, int index, size_t *sz) {
	const void * buffer = NULL;
	int t = lua_type(L, index);
	if (t == LUA_TSTRING) {
		buffer = lua_tolstring(L, index, sz);
	} else {
		if (t != LUA_TUSERDATA && t != LUA_TLIGHTUSERDATA) {
			luaL_argerror(L, index, "Need a string or userdata");
			return NULL;
		}
		buffer = lua_touserdata(L, index);
		*sz = luaL_checkinteger(L, index+1);
	}
	return buffer;
}

static inline void
write_size(uint8_t * buffer, int len) {
	buffer[0] = (len >> 8) & 0xff;
	buffer[1] = len & 0xff;
}


static int
encode(lua_State *L) {
	size_t len;
	void* p = getbuffer(L,1,&len);
	if (len >= 0x10000-PACKAGE_HEAD_SIZE) {
		return luaL_error(L, "Invalid size (too long) of data : %d", (int)len);
	}
	size_t body_len = len;
	len += PACKAGE_HEAD_SIZE;

	uint8_t* buffer = (uint8_t*)skynet_malloc(len+2);
	write_size(buffer, len); //length
	uint8_t* ptr = buffer + 2;
	int index = 3;
	for(int i=0;i < 4;++i)
	{
		int sz = luaL_checkinteger(L,index+i);
		write_size(ptr + i*2 ,sz);
	}
	memcpy(ptr + PACKAGE_HEAD_SIZE, p, body_len);
	lua_pushlstring(L,buffer,len+2);

	skynet_free(buffer);

	return 1;
}

static int
decode(lua_State *L) {
	size_t sz;
	void* msg = getbuffer(L,1,&sz);
	if (sz >= 0x10000) {
		return luaL_error(L, "Invalid size (too long) of data : %d", (int)sz);
	}
	char* p = msg;
	msg += PACKAGE_HEAD_SIZE;
	sz  -= PACKAGE_HEAD_SIZE;
	lua_pushlstring(L,msg,sz);
	for(int i = 0; i < 4; ++i)
	{
		unsigned short v = *((unsigned short*)p);
		v = NET_TO_HOST_UINT16(v);
		lua_pushinteger(L,v);
		p += 2;
	}
	return 1+4;
}

static int
decode_head(lua_State *L) {
	size_t sz;
	void* msg = getbuffer(L,1,&sz);
	if (sz >= 0x10000) {
		return luaL_error(L, "Invalid size (too long) of data : %d", (int)sz);
	}
	char* p = msg;
	for(int i = 0; i < 4; ++i)
	{
		unsigned short v = *((unsigned short*)p);  
		v = NET_TO_HOST_UINT16(v);
		lua_pushinteger(L,v);
		p += 2;
	}
	return 4;
}

int
luaopen_package_proto(lua_State *L) {

	luaL_Reg reg[] = {
		{"decode_head", decode_head },
		{"decode", decode },
		{"encode", encode },
		{NULL,NULL},
	};
	luaL_checkversion(L);
	luaL_newlib(L, reg);
	return 1;
}
