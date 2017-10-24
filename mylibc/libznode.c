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

enum 
{
	ZNODE_CB_VOID_COMPLETION = 1,
	ZNODE_CB_STAT_COMPLETION = 2,
	ZNODE_CB_DATA_COMPLETION = 3,
	ZNODE_CB_STRING_COMPLETION = 4,
	ZNODE_CB_STRINGS_COMPLETION = 5,
	ZNODE_CB_STRINGS_STAT_COMPLETION = 6,
};

enum
{
	ZNODE_OP_CREATE = 1,
	ZNODE_OP_DELETE = 2,
	ZNODE_OP_SET = 3,
	ZNODE_OP_GET = 4,
	ZNODE_EXISTS = 5,
	ZNODE_GET_CHILDREN = 6,
};

//--------------------------------------------------------
//监视事件
static struct watch_event_node{
	struct watch_event_node* next;
	int type;
	int state;
	char* path;
	int path_len;
}; 

static struct watch_event_node* 
watch_event_node_create(){
	struct watch_event_node* node = (struct watch_event_node*)skynet_malloc(sizeof(struct watch_event_node));
	node->next = NULL;
	node->type = 0;
	node->state = 0;
	node->path = NULL;
	node->path_len = 0;
	return node;
};

static void
watch_event_node_free(struct watch_event_node* node){
	if(node->path_len > 0){
		skynet_free(node->path);
	}
	skynet_free(node);
}

//-----------------------------------------------------------------
//监视路径
static struct watch_path_node{
	struct watch_path_node* next;
	char* path;
	int is_watch_child;
};

static struct watch_path_node*
watch_path_node_create(const char* watch_path,int is_watch_child){
	struct watch_path_node* node = (struct watch_path_node*)skynet_malloc(sizeof(struct watch_path_node));
	node->next = NULL;
	node->path = NULL;
	node->is_watch_child = is_watch_child;
	size_t size = strlen(watch_path);
	node->path = (char*)skynet_malloc(size+1);
	memcpy(node->path,watch_path,size);
	node->path[size] = '\0';
	return node;
}

static void
watch_path_node_free(struct watch_path_node* node) {
	if(node->path){
		skynet_free(node->path);
	}
	skynet_free(node);
}

//--------------------------------------------------------------

struct watch_path_mgr{
	struct watch_path_node* head;
};

static void 
watch_path_mgr_init(struct watch_path_mgr* mgr){
	mgr->head = NULL;
}

static void 
watch_path_mgr_uninit(struct watch_path_mgr* mgr) {
	if(mgr->head){
		struct watch_path_node* node = mgr->head;
		while(node){
			struct watch_path_node* next = node->next;
			watch_path_node_free(node);
			node = next;
		}
	}
}

static int
watch_path_add(struct watch_path_mgr* mgr,const char* path,int is_watch_child) {
	if(!mgr->head) {
		struct watch_path_node* node = watch_path_node_create(path,is_watch_child);
		mgr->head = node;
		return 0;
	}
	struct watch_path_node* node = mgr->head;
	struct watch_path_node* last_node = node;
	while(node)
	{
		if(node->path && 0==memcmp(path,node->path,strlen(path)))
		{
			return 1;  //already exist;
		}
		last_node = node;
		node = node->next;
	}
	struct watch_path_node* new_node = watch_path_node_create(path,is_watch_child);
	last_node->next = new_node;
	return 0;
}

static void
watch_path_remove(struct watch_path_mgr* mgr,const char* path) {
	if(!mgr->head) {
		return;
	}
	struct watch_path_node* node = mgr->head;
	struct watch_path_node* last_node = node;
	while(node)
	{
		if(node->path && 0 == memcmp(path,node->path,strlen(path)))
		{
			last_node->next = node->next;
			watch_path_node_free(node);
			return;
		}
		last_node = node;
	}
}

//------------------------------------------------------------------
//异步处理附加数据结构
static struct asyn_data
{
	struct asyn_data* next;

	int   session;
	int   op_type;
	char* path;
	void* znode;

	//result
	int complet_type;

	int rc;
	int version;

	union {
		struct {
			int   	value_len;
			char* 	value;
		} data;

		struct {
			int 	count;
			char** 	data;
		} strings;
	};
};

static struct asyn_data* 
asyn_data_create(int session,int op_type,char* path,void* znode){
	struct asyn_data* data = (struct asyn_data*)skynet_malloc(sizeof(struct asyn_data));
	data->session = session;
	data->op_type = op_type;
	data->znode = znode;
	data->path = NULL;
	data->next = NULL;
	data->rc = -1;
	data->version = 0;
	data->complet_type = ZNODE_CB_VOID_COMPLETION; 
	
	size_t path_len = strlen(path);
	if(path_len > 0)
	{
		data->path = skynet_malloc(path_len+1);
		memcpy(data->path,path,path_len);
		data->path[path_len] = '\0';
		//printf("\r\nasyn_data_create(),session:%d,path:%s\r\n",data->session,data->path);
	}
	data->data.value = NULL;
	data->strings.count = 0;
	data->strings.data = NULL;
	//printf("\r\nasync data create session:%d\r\n",data->session);
	return data;
}

static void 
asyn_data_free(struct asyn_data* data){
	if(data->path)
	{
		skynet_free(data->path);
	}
	if(data->complet_type == ZNODE_CB_DATA_COMPLETION
	 ||data->complet_type == ZNODE_CB_STRING_COMPLETION)
	{
		if(data->data.value)
		{
			skynet_free(data->data.value);
		}
	}
	else if(data->complet_type == ZNODE_CB_STRINGS_COMPLETION)
	{
		for(int i = 0;i < data->strings.count;++i)
		{
			skynet_free(data->strings.data[i]);
		}
		if(data->strings.data)
		{
			skynet_free(data->strings.data);
		}
	}
	skynet_free(data);
}

//thread safe queue
static struct asyn_data_queue {
	struct 	asyn_data* head;
	size_t node_size;
	pthread_mutex_t mutex;
};

static void 
asyn_data_queue_init(struct asyn_data_queue* queue){
	queue->node_size = 0;
	queue->head = NULL;
	pthread_mutexattr_t attr;
	pthread_mutexattr_init(&attr);
	pthread_mutex_init(&queue->mutex,&attr);
}

static void 
asyn_data_queue_push(struct asyn_data_queue* queue,struct asyn_data* data){
	pthread_mutex_lock(&queue->mutex);
	++(queue->node_size);
	if(!queue->head)
	{
		queue->head = data;
		pthread_mutex_unlock(&queue->mutex);
		return;
	}
	struct asyn_data* next = queue->head;
	while(next->next)
	{
		next = next->next;
	}
	next->next = data;
	pthread_mutex_unlock(&queue->mutex);
}

static struct asyn_data*
asyn_data_queue_pop(struct asyn_data_queue* queue) {
	pthread_mutex_lock(&queue->mutex);
	--(queue->node_size);
	if(!queue->head)
	{
		pthread_mutex_unlock(&queue->mutex);
		return NULL;
	}
	struct asyn_data* node = queue->head;
	queue->head = node->next;
	pthread_mutex_unlock(&queue->mutex);
	return node;
}

static void 
asyn_data_queue_unint(struct asyn_data_queue* queue){
	for(;;)
	{
		struct asyn_data* data = asyn_data_queue_pop(&queue);
		if(!data)
		{
			break;	
		}
		asyn_data_free(data);
	}
}

//----------------------------------------------------------------
//thread safe queue
static struct watch_event_queue{
	int    node_size;  		//for debug
	struct watch_event_node* head;
	pthread_mutex_t mutex;
};

static void 
watch_event_push(struct watch_event_queue* queue,struct watch_event_node* node) {
	//mutex
	pthread_mutex_lock(&queue->mutex);
	++(queue->node_size);
	if(!queue->head)
	{
		queue->head = node;
		pthread_mutex_unlock(&queue->mutex);
		return;
	}

	struct watch_event_node* next = queue->head;
	while(next->next)
	{
		next = next->next;
	}
	next->next = node;
	pthread_mutex_unlock(&queue->mutex);
}

static struct watch_event_node*
watch_event_pop(struct watch_event_queue* queue){
	pthread_mutex_lock(&queue->mutex);
	--(queue->node_size);
	if(!queue->head)
	{
		pthread_mutex_unlock(&queue->mutex);
		return NULL;
	}
	struct watch_event_node* node = queue->head;
	queue->head = node->next;
	pthread_mutex_unlock(&queue->mutex);
	return node;
}

//---------------------------------------------------------

//znode节点对象
static struct znode_t {
	zhandle_t*  zkhandle;

	//watch event
	int 		watch_cb;
	struct 		watch_event_queue queue;
	struct 		watch_path_mgr    watch_path;

	//for asyn api
	struct      asyn_data_queue asyn_data_queue;  
	int 		void_completion_cb;
	int 		stat_completion_cb;
	int			data_completion_cb;
	int 		string_completion_cb;
	int 		strings_completion_cb;
	int 		strings_stat_completion_cb;
};

static struct znode_t* 
_znode_create(){
	struct znode_t* node = (struct znode_t*)skynet_malloc(sizeof(struct znode_t));
	node->zkhandle = NULL;
	node->watch_cb = 0;
	node->queue.node_size = 0;
	node->queue.head = NULL;
	node->void_completion_cb = 0;
	node->stat_completion_cb = 0;
	node->data_completion_cb = 0;
	node->string_completion_cb = 0;
	node->strings_completion_cb = 0;
	node->strings_stat_completion_cb = 0;
	pthread_mutexattr_t  attr;
	pthread_mutexattr_init(&attr);
	pthread_mutex_init(&node->queue.mutex, &attr);
	watch_path_mgr_init(&node->watch_path);
	asyn_data_queue_init(&node->asyn_data_queue);
	return node;
}

static void
_znode_free(lua_State* L,struct znode_t* node){
	for(;;)
	{
		struct watch_event_node* event = watch_event_pop(&node->queue);
		if(!event)
		{
			break;	
		}
		watch_event_node_free(event);
	}
	if(node->watch_cb)
	{
		luaL_unref(L, LUA_REGISTRYINDEX, node->watch_cb);
	}
	if(node->void_completion_cb)
	{
		luaL_unref(L, LUA_REGISTRYINDEX, node->void_completion_cb);
	}
	if(node->stat_completion_cb)
	{
		luaL_unref(L, LUA_REGISTRYINDEX, node->stat_completion_cb);
	}
	if(node->data_completion_cb)
	{
		luaL_unref(L, LUA_REGISTRYINDEX, node->data_completion_cb);
	}
	if(node->string_completion_cb)
	{
		luaL_unref(L, LUA_REGISTRYINDEX, node->string_completion_cb);
	}
	if(node->strings_completion_cb)
	{
		luaL_unref(L, LUA_REGISTRYINDEX, node->strings_completion_cb);
	}
	if(node->strings_stat_completion_cb)
	{
		luaL_unref(L, LUA_REGISTRYINDEX, node->strings_stat_completion_cb);
	}
	watch_path_mgr_uninit(&node->watch_path);
	asyn_data_queue_unint(&node->asyn_data_queue);
	pthread_mutex_destroy(&node->queue.mutex);
	skynet_free(node);
}

static int set_watch(struct znode_t* node,const char* path);
static int set_watch_child(struct znode_t* node,const char* path);

static void 
znode_watcher_cb(zhandle_t* zh, int type, int state, const char* path, void* context){
	
	printf("\r\n ----->> znode_watcher_cb type:%d,state:%d\r\n",type,state);

	struct znode_t* znode = (struct znode_t*)context;
	if(!znode || znode->watch_cb == 0){
		return;
	}
	struct watch_event_node* event = watch_event_node_create();
	event->type = type;
	event->state = state;
	if(path)
	{
		event->path_len = strlen(path);
		event->path = (char*)skynet_malloc(event->path_len+1);
		memcpy(event->path,path,event->path_len);
		event->path[event->path_len] = '\0';
	}
	watch_event_push(&znode->queue,event);
	//需要再次注册侦听，zookeeper侦听是一次性事件
	if(type == ZOO_CREATED_EVENT)
	{
		printf("\r\n create znode event! \r\n");
		int ret = set_watch(znode,path);	
	}
	else if(type == ZOO_DELETED_EVENT)
	{
		printf("\r\n delete znode event! \r\n");
		int ret = set_watch(znode,path);		
	}
	else if(type == ZOO_CHANGED_EVENT)
	{
		printf("\r\n znode changed! \r\n");
		int ret = set_watch(znode,path);	
	}
	else if(type == ZOO_CHILD_EVENT)
	{
		printf("\r\n znode child event \r\n");
		int ret = set_watch_child(znode,path);
	}
	else if(type == ZOO_SESSION_EVENT)
	{
		if(state == ZOO_EXPIRED_SESSION_STATE)
		{
			printf("\r\n expired session state \r\n");
			zoo_set_watcher(znode->zkhandle,znode_watcher_cb);
			//各种侦听事件
			if(znode->watch_path.head) {
				struct watch_path_node* n = znode->watch_path.head;
				while(n)
				{
					if(n->path)
					{
						if(n->is_watch_child)
							set_watch_child(znode,n->path);
						else
							set_watch(znode,n->path);
					}
					n = n->next;
				}
			}
		}
		else if(state == ZOO_AUTH_FAILED_STATE)
		{
			printf("\r\n auth failed state \r\n");
		}
		else if(state == ZOO_CONNECTING_STATE)
		{
			printf("\r\n connecting state \r\n");
		}
		else if(state == ZOO_ASSOCIATING_STATE)
		{
			printf("\r\n associating state \r\n");
		}
		else if(state == ZOO_CONNECTED_STATE)
		{
			printf("\r\n connected state \r\n");
		}
		else
		{
			printf("\r\n znode_watcher_cb state error? type:%d,state:%d\r\n",type,state);
		}
	}
	else
	{
		printf("\r\n znode_watcher_cb type error? type:%d,state:%d\r\n",type,state);	
	}
}

static void 
default_stat_completion(int rc,const struct Stat *stat,const void *data){
	printf("\r\ndefault_stat_completion(),rc:%d\r\n",rc);
}

static int 
set_watch(struct znode_t* node,const char* path){
	//int ret = zoo_wexists(node->zkhandle, path, znode_watcher_cb, node, NULL);
	int ret = zoo_aexists(node->zkhandle,path,1,default_stat_completion,NULL);
	return ret;
}

static void 
default_strings_completion (int rc,
	const struct String_vector *strings, const void *data){
	printf("\r\ndefault_strings_completion(),rc:%d\r\n",rc);
}

static int 
set_watch_child(struct znode_t* node,const char* path){
	//int ret = zoo_wget_children(node->zkhandle, path, znode_watcher_cb,node,NULL);
	int ret = zoo_aget_children(node->zkhandle,path,1,default_strings_completion,NULL);
	return ret;
}

static void 
znode_void_completion_cb(int rc, const void *data){
	if(data == NULL) return;
	struct asyn_data* d = (struct asyn_data*)data;
	struct znode_t* znode = (struct znode_t*)d->znode;
	d->complet_type = ZNODE_CB_VOID_COMPLETION;
	d->rc = rc;
	asyn_data_queue_push(&znode->asyn_data_queue,d);
}

static void 
znode_stat_completion_cb(int rc, const struct Stat *stat,const void *data){
	if(data == NULL) return;
	struct asyn_data* d = (struct asyn_data*)data;
	struct znode_t* znode = (struct znode_t*)d->znode;
	d->complet_type = ZNODE_CB_STAT_COMPLETION;
	d->rc = rc;
	if(stat) 
	{
		d->version = stat->version;
	}
	asyn_data_queue_push(&znode->asyn_data_queue,d);
}

static void 
znode_data_completion_cb(int rc, const char *value, int value_len,
	const struct Stat *stat, const void *data){
	if(data == NULL) return;
	struct asyn_data* d = (struct asyn_data*)data;
	struct znode_t* znode = (struct znode_t*)d->znode;
	d->complet_type = ZNODE_CB_DATA_COMPLETION;
	d->rc = rc;
	if(stat) 
	{
		d->version = stat->version;
	}
	if(value_len > 0)
	{
		d->data.value_len = value_len;
		d->data.value = (char*)skynet_malloc(value_len);
		memcpy(d->data.value,value,value_len);
	}
	asyn_data_queue_push(&znode->asyn_data_queue,d);
}

static void 
znode_string_completion_cb(int rc, const char *value, const void *data){
	if(data == NULL) return;
	struct asyn_data* d = (struct asyn_data*)data;
	struct znode_t* znode = (struct znode_t*)d->znode;
	d->complet_type = ZNODE_CB_STRING_COMPLETION;
	d->rc = rc;
	int value_len = strlen(value);
	d->data.value_len = value_len;
	d->data.value = (char*)skynet_malloc(value_len+1);
	memcpy(d->data.value,value,value_len);
	d->data.value[value_len] = '\0';
	asyn_data_queue_push(&znode->asyn_data_queue,d);
}

static void 
znode_strings_completion_cb(int rc,
	const struct String_vector *strings, const void *data){
	if(data == NULL) return;
	struct asyn_data* d = (struct asyn_data*)data;
	struct znode_t* znode = (struct znode_t*)d->znode;
	d->complet_type = ZNODE_CB_STRINGS_COMPLETION;
	d->rc = rc;
	if(strings->count > 0)
	{
		d->strings.count = strings->count;
		d->strings.data = (char**)skynet_malloc(sizeof(char*)*strings->count);
		for(int i=0;i<strings->count;++i)
		{
			int value_len = strlen(strings->data[i]);
			d->strings.data[i] = (char*)skynet_malloc(value_len+1);
			memcpy(d->strings.data[i],strings->data[i],value_len);
			d->strings.data[i][value_len] = '\0';
		}
	}
	asyn_data_queue_push(&znode->asyn_data_queue,d);
}

static void 
znode_strings_stat_completion_cb(int rc,
	const struct String_vector *strings,const struct Stat* stat, const void *data){
	if(data == NULL) return;
	struct asyn_data* d = (struct asyn_data*)data;
	struct znode_t* znode = (struct znode_t*)d->znode;
	d->complet_type = ZNODE_CB_STRINGS_STAT_COMPLETION;
	d->rc = rc;
	if(stat)
	{
		d->version = stat->version;
	}
	if(strings->count > 0)
	{
		d->strings.count = strings->count;
		d->strings.data = (char**)skynet_malloc(sizeof(char*)*strings->count);
		for(int i=0;i<strings->count;++i)
		{
			int value_len = strlen(strings->data[i]);
			d->strings.data[i] = (char*)skynet_malloc(value_len+1);
			memcpy(d->strings.data[i],strings->data[i],value_len);
			d->strings.data[i][value_len] = '\0';
		}
	}
	//printf("\r\n ***************,session:%d,path:%s\r\n",d->session,d->path);
	asyn_data_queue_push(&znode->asyn_data_queue,d);
}

static int
znode_set_debug_level(lua_State* L) {
	int level = lua_tointeger(L,1);
	zoo_set_debug_level(level);
	return 0;
}

static int
znode_init(lua_State* L) {
	struct znode_t* node = _znode_create();
	const char* host = lua_tostring(L,1);
	int timeout = lua_tointeger(L,2);
	//must be stack top!
	node->watch_cb = luaL_ref(L, LUA_REGISTRYINDEX);
	node->zkhandle = zookeeper_init(host, znode_watcher_cb, timeout, 0,node, 0);
	if(!node->zkhandle){
		skynet_free(node);
		return 0;
	}
	lua_pushlightuserdata(L,node);
	return 1;
}

static int
znode_close(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	zookeeper_close(node->zkhandle);
	_znode_free(L,node);
	return 0;
}

static void
_handle_asyn_event(lua_State* L,struct znode_t* znode,struct asyn_data* d) {
	if(d->complet_type == ZNODE_CB_VOID_COMPLETION){
		if(znode->void_completion_cb){
			lua_rawgeti(L, LUA_REGISTRYINDEX, znode->void_completion_cb);
			lua_pushlightuserdata(L,znode);
			lua_pushinteger(L,d->session);
			lua_pushinteger(L,d->op_type);
			if(d->path)
				lua_pushstring(L,d->path);
			else
				lua_pushnil(L);
			lua_pushinteger(L,d->rc);
			lua_call(L, 5, 0);
		}
	}else if(d->complet_type == ZNODE_CB_STAT_COMPLETION){
		if(znode->stat_completion_cb){
			lua_rawgeti(L, LUA_REGISTRYINDEX, znode->stat_completion_cb);
			lua_pushlightuserdata(L,znode);
			lua_pushinteger(L,d->session);
			lua_pushinteger(L,d->op_type);
			if(d->path)
				lua_pushstring(L,d->path);
			else
				lua_pushnil(L);
			lua_pushinteger(L,d->rc);
			lua_pushinteger(L,d->version);
			lua_call(L, 6, 0);
		}
	}else if(d->complet_type == ZNODE_CB_DATA_COMPLETION){
		if(znode->data_completion_cb){
			lua_rawgeti(L, LUA_REGISTRYINDEX, znode->data_completion_cb);
			lua_pushlightuserdata(L,znode);
			lua_pushinteger(L,d->session);
			lua_pushinteger(L,d->op_type);
			if(d->path)
				lua_pushstring(L,d->path);
			else
				lua_pushnil(L);
			lua_pushinteger(L,d->rc);
			if(d->data.value_len > 0)
				lua_pushlstring(L,d->data.value,d->data.value_len);
			else
				lua_pushnil(L);
			lua_pushinteger(L,d->version);
			lua_call(L, 7, 0);
		}
	}else if(d->complet_type == ZNODE_CB_STRING_COMPLETION){
		if(znode->string_completion_cb){
			lua_rawgeti(L, LUA_REGISTRYINDEX, znode->string_completion_cb);
			lua_pushlightuserdata(L,znode);
			lua_pushinteger(L,d->session);
			lua_pushinteger(L,d->op_type);
			if(d->path)
				lua_pushstring(L,d->path);
			else
				lua_pushnil(L);
			lua_pushinteger(L,d->rc);
			if(d->data.value_len > 0)
				lua_pushlstring(L,d->data.value,d->data.value_len);
			else
				lua_pushnil(L);
			lua_call(L, 6, 0);
		}
	}else if(d->complet_type == ZNODE_CB_STRINGS_COMPLETION){
		if(znode->strings_completion_cb){
			lua_rawgeti(L, LUA_REGISTRYINDEX, znode->strings_completion_cb);
			lua_pushlightuserdata(L,znode);
			lua_pushinteger(L,d->session);
			lua_pushinteger(L,d->op_type);
			if(d->path)
				lua_pushstring(L,d->path);
			else
				lua_pushnil(L);
			lua_pushinteger(L,d->rc);
			lua_newtable(L);
			for(int i=0;i<d->strings.count;++i)
			{
				lua_pushstring(L,d->strings.data[i]);
				lua_seti(L,-2,i+1);
			}
			lua_call(L, 6, 0);
		}
	}
	else if(d->complet_type == ZNODE_CB_STRINGS_STAT_COMPLETION) {
		if(znode->strings_stat_completion_cb){
			lua_rawgeti(L, LUA_REGISTRYINDEX, znode->strings_stat_completion_cb);
			lua_pushlightuserdata(L,znode);
			lua_pushinteger(L,d->session);
			lua_pushinteger(L,d->op_type);
			if(d->path)
				lua_pushstring(L,d->path);
			else
				lua_pushnil(L);
			lua_pushinteger(L,d->rc);
			lua_pushinteger(L,d->version);
			lua_newtable(L);
			for(int i=0;i<d->strings.count;++i)
			{
				lua_pushstring(L,d->strings.data[i]);
				lua_seti(L,-2,i+1);
			}
			lua_call(L, 7, 0);
		}
	}
	else{
		printf("invaild !!!");
	}
}

static int 
znode_update(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	int i = 0;
	for(;;++i)
	{
		struct watch_event_node* event = watch_event_pop(&node->queue);
		if(!event)
		{
			break;
		}
		lua_rawgeti(L, LUA_REGISTRYINDEX, node->watch_cb);
		lua_pushlightuserdata(L,node);
		lua_pushinteger(L,event->type);
		lua_pushinteger(L,event->state);
		if(event->path_len > 0){
			lua_pushlstring(L,event->path,event->path_len);
		}else{
			lua_pushnil(L);
		}
		lua_call(L, 4, 0);
		watch_event_node_free(event);
	}
	lua_pushinteger(L,i);
	i=0;
	for(;;++i)
	{
		struct asyn_data* data = asyn_data_queue_pop(&node->asyn_data_queue);
		if(!data)
		{
			break;
		}
		//printf("\r\nasync data handle session:%d,path:%s\r\n",data->session,data->path);
		_handle_asyn_event(L,node,data);
		asyn_data_free(data);
	}
	lua_pushinteger(L,i);
	return 2;
}

//同步-----------------------
static int 
znode_create(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	size_t path_len;
	const char* path = lua_tolstring(L,2,&path_len);
	size_t len;
	const char* value = lua_tolstring(L,3,&len);
	int flag = lua_tointeger(L,4);
	int rc = zoo_create(node->zkhandle,path,value,len,
		&ZOO_OPEN_ACL_UNSAFE,flag,path,path_len);
	lua_pushinteger(L,rc);
	return 1;
}

static int 
znode_exists(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	int flag = zoo_exists(node->zkhandle,path,1,NULL);
	lua_pushinteger(L,flag);
	return 1;
}

static int 
znode_delete(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	int version = lua_tointeger(L,3);
	int flag = zoo_delete(node->zkhandle,path,version);
	lua_pushinteger(L,flag);
	return 1;
}

static int 
znode_get(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	char buffer[512] = {0x00};
	int len = 512;
	struct Stat stat;
	stat.version = 0;
	int flag = zoo_get(node->zkhandle,path,1,buffer,&len,&stat);
	lua_pushinteger(L,flag);
	lua_pushlstring(L,buffer,len);
	lua_pushinteger(L,stat.version);
	return 3;
}

static int
znode_set(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	int len;
	const char* buffer = lua_tolstring(L,3,&len);
	int version = lua_tointeger(L,4);
	int flag = zoo_set(node->zkhandle,path,buffer,len,version);
	lua_pushinteger(L,flag);
	return 1;
}

static int
znode_get_children(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	struct String_vector strings;
	struct Stat stat;
	strings.count = 0;
	stat.version = 0;
	int ret = zoo_get_children2(node->zkhandle,path,1,&strings,&stat);
	lua_pushinteger(L,ret);
	lua_pushinteger(L,stat.version);
	lua_newtable(L);
	for(int i=0;i<strings.count;++i)
	{
		lua_pushstring(L,strings.data[i]);
		lua_seti(L,-2,i+1);
	}
	return 3;
}

//----------------------------------------------------------------------------------

//异步-------------------------------------------------------------------------------

static int 
znode_acreate(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	size_t path_len;
	const char* path = lua_tostring(L,2);
	size_t len;
	const char* value = lua_tolstring(L,3,&len);
	int flag = lua_tointeger(L,4);
	int session = lua_tointeger(L,5);
	struct asyn_data* data = asyn_data_create(session,ZNODE_OP_CREATE,path,node);
	int rc = zoo_acreate(node->zkhandle,path,value,len,
		&ZOO_OPEN_ACL_UNSAFE,flag,znode_string_completion_cb,data);
	lua_pushinteger(L,rc);
	return 1;
}

static int 
znode_adelete(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	int version = lua_tointeger(L,3);
	int session = lua_tointeger(L,4);
	struct asyn_data* data = asyn_data_create(session,ZNODE_OP_DELETE,path,node);
	int ret = zoo_adelete(node->zkhandle,path,version,znode_void_completion_cb,data);
	lua_pushinteger(L,ret);
	return 1;
}

static int 
znode_aexists(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	int session = lua_tointeger(L,3);
	struct asyn_data* data = asyn_data_create(session,ZNODE_EXISTS,path,node);
	int ret = zoo_aexists(node->zkhandle,path,1,znode_stat_completion_cb,data);
	lua_pushinteger(L,ret);
	return 1;
}

static int 
znode_aget(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	int session = lua_tointeger(L,3);
	struct asyn_data* data = asyn_data_create(session,ZNODE_OP_GET,path,node);
	int ret = zoo_aget(node->zkhandle,path,1,znode_data_completion_cb,data);
	lua_pushinteger(L,ret);
	return 1;
}

static int
znode_aset(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	int len;
	const char* buffer = lua_tolstring(L,3,&len);
	int version = lua_tointeger(L,4);
	int session = lua_tointeger(L,5);
	struct asyn_data* data = asyn_data_create(session,ZNODE_OP_SET,path,node);
	int ret = zoo_aset(node->zkhandle,path,buffer,len,version,znode_data_completion_cb,data);
	lua_pushinteger(L,ret);
	return 1;
}

static int
znode_aget_children(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	int session = lua_tointeger(L,3);
	struct asyn_data* data = asyn_data_create(session,ZNODE_GET_CHILDREN,path,node);
	//printf("\r\n-------znode_aget_children,path:%s\r\n",path);
	int ret = zoo_aget_children2(node->zkhandle,path,1,znode_strings_stat_completion_cb,data);
	lua_pushinteger(L,ret);
	return 1;
}

//----------------------------------------------------------------------

static int
znodeext_add_watch_path(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	int is_watch_child = lua_tointeger(L,3);
	int ret = watch_path_add(&node->watch_path,path,is_watch_child);
	if(is_watch_child)
		ret = set_watch_child(node,path);
	else
		ret = set_watch(node,path);
	lua_pushinteger(L,ret);
	return 1;
}

static int
znodeext_remove_watch_path(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	const char* path = lua_tostring(L,2);
	watch_path_remove(&node->watch_path,path);
	return 0;
}

static int 
znodeext_set_asyn_completion_callback(lua_State* L) {
	struct znode_t* node = (struct znode_t*)lua_touserdata(L,1);
	int type = lua_tointeger(L,2);
	int ret = 0;

	if(type == ZNODE_CB_VOID_COMPLETION)
	{
		node->void_completion_cb = luaL_ref(L, LUA_REGISTRYINDEX);
	}
	else if(type == ZNODE_CB_STAT_COMPLETION)
	{
		node->stat_completion_cb = luaL_ref(L, LUA_REGISTRYINDEX);
	}
	else if(type == ZNODE_CB_DATA_COMPLETION)
	{
		node->data_completion_cb = luaL_ref(L, LUA_REGISTRYINDEX);
	}
	else if(type == ZNODE_CB_STRING_COMPLETION)
	{
		node->string_completion_cb = luaL_ref(L, LUA_REGISTRYINDEX);
	}
	else if(type == ZNODE_CB_STRINGS_COMPLETION)
	{
		node->strings_completion_cb = luaL_ref(L, LUA_REGISTRYINDEX);
	}
	else if(type == ZNODE_CB_STRINGS_STAT_COMPLETION)
	{
		node->strings_stat_completion_cb = luaL_ref(L, LUA_REGISTRYINDEX);
	}
	else
	{
		ret = 1;
	}
	lua_pushinteger(L,ret);
	return 1;
}

//---------------------------------------------------------

int
luaopen_znode(lua_State *L) {

	luaL_Reg reg[] = {
		{"znode_set_debug_level",znode_set_debug_level},
		{"znode_init",znode_init},
		{"znode_close",znode_close},
		{"znode_update",znode_update},

		//同步接口,注意使用场景
		{"znode_create",znode_create},
		{"znode_delete",znode_delete},
		{"znode_exists",znode_exists},
		{"znode_get",znode_get},
		{"znode_set",znode_set},
		{"znode_get_children",znode_get_children},

		//异步接口,主要使用
		{"znode_acreate",znode_acreate},
		{"znode_adelete",znode_adelete},
		{"znode_aexists",znode_aexists},
		{"znode_aget",znode_aget},
		{"znode_aset",znode_aset},
		{"znode_aget_children",znode_aget_children},

		//额外扩展功能
		{"znodeext_add_watch_path",znodeext_add_watch_path},
		{"znodeext_remove_watch_path",znodeext_remove_watch_path},
		{"znodeext_set_asyn_completion_callback",znodeext_set_asyn_completion_callback},
		{NULL,NULL},
	};

	luaL_checkversion(L);
	luaL_newlib(L, reg);
	return 1;
}
