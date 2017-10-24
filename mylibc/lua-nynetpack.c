#define LUA_LIB

#include "skynet_malloc.h"

#include "skynet_socket.h"

#include <lua.h>
#include <lauxlib.h>

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define QUEUESIZE 1024
#define HASHSIZE 4096
#define SMALLSTRING 2048

#define TYPE_DATA 1
#define TYPE_MORE 2
#define TYPE_ERROR 3
#define TYPE_OPEN 4
#define TYPE_CLOSE 5
#define TYPE_WARNING 6


#define NY_HEAD_SIZE 10    //package head id:2  ,  lenght:4   , connect_id:4

//void* my_malloc(size_t size)
//{
	//printf("\n\r my_malloc size:%d\n",size);
	//assert(size < 256*1024);
//	return skynet_malloc(size);
//}

#define SKYNET_MALLOC skynet_malloc

struct netpack {
	int id;
	int size;
	void * buffer;
	int type_id;
	int conn_id;
};

struct uncomplete {
	struct netpack pack;
	struct uncomplete * next;
	int read;
	//int header;

	int type_id;
	int conn_id;
};

struct queue {
	int cap;
	int head;
	int tail;
	struct uncomplete * hash[HASHSIZE];
	struct netpack queue[QUEUESIZE];
};

static uint32_t
bytes_to_uint32(uint8_t* bytes) {
	uint32_t n = 0;
	n |= (uint32_t)bytes[0];
	n |= ((uint32_t)bytes[1] << 8);
	n |= ((uint32_t)bytes[2] << 16);
	n |= ((uint32_t)bytes[3] << 24);
	return n;
}

static uint16_t
bytes_to_uint16(uint8_t* bytes) {
	uint16_t n = 0;
	n |= (uint16_t)bytes[0];
	n |= ((uint16_t)bytes[1] << 8);
	return n;
}

static void
clear_list(struct uncomplete * uc) {
	while (uc) {
		skynet_free(uc->pack.buffer);
		void * tmp = uc;
		uc = uc->next;
		skynet_free(tmp);
	}
}

static int
lclear(lua_State *L) {
	struct queue * q = lua_touserdata(L, 1);
	if (q == NULL) {
		return 0;
	}
	int i;
	for (i=0;i<HASHSIZE;i++) {
		clear_list(q->hash[i]);
		q->hash[i] = NULL;
	}
	if (q->head > q->tail) {
		q->tail += q->cap;
	}
	for (i=q->head;i<q->tail;i++) {
		struct netpack *np = &q->queue[i % q->cap];
		skynet_free(np->buffer);
	}
	q->head = q->tail = 0;

	return 0;
}

static inline int
hash_fd(int fd) {
	int a = fd >> 24;
	int b = fd >> 12;
	int c = fd;
	return (int)(((uint32_t)(a + b + c)) % HASHSIZE);
}

static struct uncomplete *
find_uncomplete(struct queue *q, int fd) {
	if (q == NULL)
	{
		return NULL;
	}
	int h = hash_fd(fd);
	struct uncomplete * uc = q->hash[h];
	if (uc == NULL)
	{
		return NULL;
	}
	if (uc->pack.id == fd) {
		q->hash[h] = uc->next;
		return uc;
	}
	struct uncomplete * last = uc;
	while (last->next) {
		uc = last->next;
		if (uc->pack.id == fd) {
			last->next = uc->next;
			return uc;
		}
		last = uc;
	}
	return NULL;
}

static struct queue *
get_queue(lua_State *L) {
	struct queue *q = lua_touserdata(L,1);
	if (q == NULL) {
		q = lua_newuserdata(L, sizeof(struct queue));
		q->cap = QUEUESIZE;
		q->head = 0;
		q->tail = 0;
		int i;
		for (i=0;i<HASHSIZE;i++) {
			q->hash[i] = NULL;
		}
		lua_replace(L, 1);
	}
	return q;
}

static void
expand_queue(lua_State *L, struct queue *q) {
	//printf("expand_queue!");
	struct queue *nq = lua_newuserdata(L, sizeof(struct queue) + q->cap * sizeof(struct netpack));
	nq->cap = q->cap + QUEUESIZE;
	nq->head = 0;
	nq->tail = q->cap;
	memcpy(nq->hash, q->hash, sizeof(nq->hash));
	memset(q->hash, 0, sizeof(q->hash));
	int i;
	for (i=0;i<q->cap;i++) {
		int idx = (q->head + i) % q->cap;
		nq->queue[i] = q->queue[idx];
	}
	q->head = q->tail = 0;
	lua_replace(L,1);
}

static void
push_data(lua_State *L, int fd, void *buffer, int size, int clone,int type_id,int conn_id) {
	if (clone) {
		assert(size < 256*1024);
		void * tmp = SKYNET_MALLOC(size);
		memcpy(tmp, buffer, size);
		buffer = tmp;
	}
	struct queue *q = get_queue(L);
	struct netpack *np = &q->queue[q->tail];
	np->type_id = type_id;
	np->conn_id = conn_id;

	if (++q->tail >= q->cap)
		q->tail -= q->cap;

	np->id = fd;
	np->buffer = buffer;
	np->size = size;
	if (q->head == q->tail) {
		expand_queue(L, q);
	}
}

static struct uncomplete *
save_uncomplete(lua_State *L, int fd) {
	struct queue *q = get_queue(L);
	int h = hash_fd(fd);
	size_t size = sizeof(struct uncomplete);
	struct uncomplete * uc = SKYNET_MALLOC(size);
	memset(uc, 0, size);
	uc->next = q->hash[h];
	uc->pack.id = fd;
	q->hash[h] = uc;

	return uc;
}

/*
static inline int
read_size(uint8_t * buffer) {
	int r = (int)buffer[0] << 8 | (int)buffer[1];
	return r;
}
*/

static void
push_more(lua_State *L, int fd, uint8_t *buffer, int size) {
	if (size < NY_HEAD_SIZE) {

		struct uncomplete * uc = save_uncomplete(L, fd);
		uc->read = -1;
		//uc->header = *buffer;

		uc->pack.size = size;
		uc->pack.buffer = SKYNET_MALLOC(16);  //减少碎片
		memcpy(uc->pack.buffer, buffer, size);

		return;
	}

	uint16_t type_id = bytes_to_uint16(buffer);
	int pack_size = bytes_to_uint32(buffer + 2);
	uint32_t conn_id = bytes_to_uint32(buffer + 2 + 4);
	pack_size -= NY_HEAD_SIZE;

	buffer += NY_HEAD_SIZE;
	size -= NY_HEAD_SIZE;

	if (size < pack_size) {

		struct uncomplete * uc = save_uncomplete(L, fd);
		uc->read = size;
		uc->pack.size = pack_size;
		uc->type_id = type_id;
		uc->conn_id = conn_id;
		uc->pack.buffer = SKYNET_MALLOC(pack_size);  
		memcpy(uc->pack.buffer, buffer, size);
		return;
	}

	push_data(L, fd, buffer, pack_size, 1, type_id, conn_id);

	buffer += pack_size;
	size -= pack_size;
	if (size > 0) {
		push_more(L, fd, buffer, size);
	}
}

static void
close_uncomplete(lua_State *L, int fd) {

	struct queue *q = lua_touserdata(L,1);
	struct uncomplete * uc = find_uncomplete(q, fd);
	if (uc) {
		skynet_free(uc->pack.buffer);
		skynet_free(uc);
	}
}


static int
filter_data_(lua_State *L, int fd, uint8_t * buffer, int size) {

	struct queue *q = lua_touserdata(L,1);
	struct uncomplete * uc = find_uncomplete(q, fd);
	if (uc) {
		if (uc->read < 0) {
			// read size
			assert(uc->read == -1);

			uint8_t bytes[NY_HEAD_SIZE];
			memcpy(bytes, uc->pack.buffer, uc->pack.size);
			skynet_free(uc->pack.buffer);

			int _start = uc->pack.size;
			int _need = NY_HEAD_SIZE - uc->pack.size;
		  
		  uc->pack.size = 0;
		  uc->pack.buffer = (void*)0;

			for (int i = 0; i < _need; ++i)
			{
					bytes[i + _start] = buffer[i];
			}
			
			uint16_t type_id = bytes_to_uint16(bytes);
			int pack_size = bytes_to_uint32(bytes + 2);
			uint32_t conn_id = bytes_to_uint32(bytes + 2 + 4);
			
			pack_size -= NY_HEAD_SIZE;
			
			buffer += (_need);
			size -= (_need);

			uc->pack.size = pack_size;


			uc->pack.buffer = SKYNET_MALLOC(pack_size);
			uc->read = 0;
			uc->type_id = type_id;
			uc->conn_id = conn_id;
			
			printf("\r\n wwww 1 ,type_id:%d,pack_size:%d",type_id,uc->pack.size);
		}
		int need = uc->pack.size - uc->read;
		if (size < need) {
			memcpy(uc->pack.buffer + uc->read, buffer, size);
			uc->read += size;
			int h = hash_fd(fd);
			uc->next = q->hash[h];
			q->hash[h] = uc;
			return 1;
		}
		memcpy(uc->pack.buffer + uc->read, buffer, need);
		buffer += need;
		size -= need;
		if (size == 0) {
			
			printf("\r\n wwww 3! pack_size:%d,more size:%d,asdfa type_id:%d \r\n",uc->pack.size,size,uc->type_id);
			
			lua_pushvalue(L, lua_upvalueindex(TYPE_DATA));
			lua_pushinteger(L, fd);
			lua_pushlightuserdata(L, uc->pack.buffer);
			lua_pushinteger(L, uc->pack.size);

			lua_pushinteger(L, uc->type_id);
			lua_pushinteger(L, uc->conn_id);

			skynet_free(uc);
			return 5+2;
		}
		// more data
		push_data(L, fd, uc->pack.buffer, uc->pack.size, 0,uc->type_id,uc->conn_id);
		skynet_free(uc);
		push_more(L, fd, buffer, size);
		lua_pushvalue(L, lua_upvalueindex(TYPE_MORE));
		return 2;
	} else {
		
		if (size < NY_HEAD_SIZE) {
			struct uncomplete * uc = save_uncomplete(L, fd);
			uc->read = -1;
			//uc->header = *buffer;

			uc->pack.size = size;
			uc->pack.buffer = SKYNET_MALLOC(16);  //减少碎片
			memcpy(uc->pack.buffer, buffer, size);
			return 1;
		}
	
		uint16_t type_id = bytes_to_uint16(buffer);
		int pack_size = bytes_to_uint32(buffer+2);
		uint32_t conn_id = bytes_to_uint32(buffer+2+4);
		pack_size -= NY_HEAD_SIZE;
		buffer+= NY_HEAD_SIZE;
		size-= NY_HEAD_SIZE;

		if (size < pack_size) {
			struct uncomplete * uc = save_uncomplete(L, fd);
			uc->read = size;
			uc->pack.size = pack_size;
			assert(pack_size < 256*1024);
			uc->pack.buffer = SKYNET_MALLOC(pack_size);
			memcpy(uc->pack.buffer, buffer, size);

			uc->type_id = type_id;
			uc->conn_id = conn_id;
			
			return 1;
		}
		if (size == pack_size) {
			// just one package
			lua_pushvalue(L, lua_upvalueindex(TYPE_DATA));
			lua_pushinteger(L, fd);
			assert(pack_size < 256*1024);
			void * result = SKYNET_MALLOC(pack_size);
			memcpy(result, buffer, size);
			lua_pushlightuserdata(L, result);
			lua_pushinteger(L, size);
			
			lua_pushinteger(L, type_id);
			lua_pushinteger(L, conn_id);
			return 5+2;
		}
		
		// more data
		push_data(L, fd, buffer, pack_size, 1,type_id,conn_id);
		buffer += pack_size;
		size -= pack_size;
		push_more(L, fd, buffer, size);
		lua_pushvalue(L, lua_upvalueindex(TYPE_MORE));
		return 2;
	}
}

static inline int
filter_data(lua_State *L, int fd, uint8_t * buffer, int size) {
	int ret = filter_data_(L, fd, buffer, size);
	// buffer is the data of socket message, it malloc at socket_server.c : function forward_message .
	// it should be free before return,
	skynet_free(buffer);
	return ret;
}

static void
pushstring(lua_State *L, const char * msg, int size) {
	if (msg) {
		lua_pushlstring(L, msg, size);
	} else {
		lua_pushliteral(L, "");
	}
}

/*
	userdata queue
	lightuserdata msg
	integer size
	return
	userdata queue
	integer type
	integer fd
	string msg | lightuserdata/integer

	type_id
	conn_id
 */
static int
lfilter(lua_State *L) {
	struct skynet_socket_message *message = lua_touserdata(L,2);
	int size = luaL_checkinteger(L,3);
	char * buffer = message->buffer;
	if (buffer == NULL) {
		buffer = (char *)(message+1);
		size -= sizeof(*message);
	} else {
		size = -1;
	}

	lua_settop(L, 1);

	switch(message->type) {
	case SKYNET_SOCKET_TYPE_DATA:
		// ignore listen id (message->id)
		assert(size == -1);	// never padding string
		return filter_data(L, message->id, (uint8_t *)buffer, message->ud);
	case SKYNET_SOCKET_TYPE_CONNECT:
		// ignore listen fd connect
		return 1;
	case SKYNET_SOCKET_TYPE_CLOSE:
		// no more data in fd (message->id)
		close_uncomplete(L, message->id);
		lua_pushvalue(L, lua_upvalueindex(TYPE_CLOSE));
		lua_pushinteger(L, message->id);
		return 3;
	case SKYNET_SOCKET_TYPE_ACCEPT:
		lua_pushvalue(L, lua_upvalueindex(TYPE_OPEN));
		// ignore listen id (message->id);
		lua_pushinteger(L, message->ud);
		pushstring(L, buffer, size);
		return 4;
	case SKYNET_SOCKET_TYPE_ERROR:
		// no more data in fd (message->id)
		close_uncomplete(L, message->id);
		lua_pushvalue(L, lua_upvalueindex(TYPE_ERROR));
		lua_pushinteger(L, message->id);
		pushstring(L, buffer, size);
		return 4;
	case SKYNET_SOCKET_TYPE_WARNING:
		lua_pushvalue(L, lua_upvalueindex(TYPE_WARNING));
		lua_pushinteger(L, message->id);
		lua_pushinteger(L, message->ud);
		return 4;
	default:
		// never get here
		return 1;
	}
}

/*
	userdata queue
	return
	integer fd
	lightuserdata msg
	integer size
 */
static int
lpop(lua_State *L) {
	struct queue * q = lua_touserdata(L, 1);
	if (q == NULL || q->head == q->tail)
	{
		//if(q == NULL)
			//printf("q == NULL");

		//if(q->head == q->tail)
		//	printf("q->head == q->tail");

		return 0;
	}
	
	struct netpack *np = &q->queue[q->head];
	if (++q->head >= q->cap) {
		q->head = 0;
	}
	lua_pushinteger(L, np->id);
	lua_pushlightuserdata(L, np->buffer);
	lua_pushinteger(L, np->size);
	lua_pushinteger(L, np->type_id);
	lua_pushinteger(L, np->conn_id);
	return 3+2;
}

/*
	string msg | lightuserdata/integer
	lightuserdata/integer
 */

static const char *
tolstring(lua_State *L, size_t *sz, int index) {
	const char * ptr;
	if (lua_isuserdata(L,index)) {
		ptr = (const char *)lua_touserdata(L,index);
		*sz = (size_t)luaL_checkinteger(L, index+1);
	} else {
		ptr = luaL_checklstring(L, index, sz);
	}
	return ptr;
}

static inline void
write_size(uint8_t * buffer, int len) {
	buffer[0] = (len >> 8) & 0xff;
	buffer[1] = len & 0xff;
}

static int
lpack(lua_State *L) {
	size_t len;
	const char * ptr = tolstring(L, &len, 1);
	if (len >= 0x10000) {
		return luaL_error(L, "Invalid size (too long) of data : %d", (int)len);
	}

	assert(len+2 < 256*1024);
	uint8_t * buffer = SKYNET_MALLOC(len + 2);
	write_size(buffer, len);
	memcpy(buffer+2, ptr, len);

	lua_pushlightuserdata(L, buffer);
	lua_pushinteger(L, len + 2);

	return 2;
}

static int
ltostring(lua_State *L) {
	void * ptr = lua_touserdata(L, 1);
	int size = luaL_checkinteger(L, 2);
	if (ptr == NULL) {
		lua_pushliteral(L, "");
	} else {
		lua_pushlstring(L, (const char *)ptr, size);
		skynet_free(ptr);
	}
	return 1;
}

LUAMOD_API int
luaopen_skynet_nynetpack(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "pop", lpop },
		{ "pack", lpack },
		{ "clear", lclear },
		{ "tostring", ltostring },
		{ NULL, NULL },
	};
	luaL_newlib(L,l);


	// the order is same with macros : TYPE_* (defined top)
	lua_pushliteral(L, "data");
	lua_pushliteral(L, "more");
	lua_pushliteral(L, "error");
	lua_pushliteral(L, "open");
	lua_pushliteral(L, "close");
	lua_pushliteral(L, "warning");

	lua_pushcclosure(L, lfilter, 6);
	lua_setfield(L, -2, "filter");

	return 1;
}
