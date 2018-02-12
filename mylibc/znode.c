

#include "znode.h"
#include "linked.h"
#include "safe_queue.h"
#include "smemory.h"
#include "uthash.h"
#include "spinlock.h"

#include <stdio.h>

#include <zookeeper.h>
#include <zookeeper_log.h>

//-----------------------private struct----------------------------------

enum info_type{
	info_type_event = 0,
	info_type_data,
};

struct znode_event_t {
	struct node_t node;
	enum info_type  info_type;
	union union_info{
		struct znode_event_info_t event_info;
		struct znode_data_info_t  data_info;
	}info;
};

struct znode_data_session_t {
	int 	session_;
	int 	op_type_;
	char* 	path_;
	void* 	znode_;
	unsigned int  time_;
	UT_hash_handle hh;
};

struct znode_watch_path_t {
	char* path;
	int   is_watch_child;
	unsigned short  id;
	unsigned short  is_watched;
	UT_hash_handle hh;
};

struct znode_t{
	zhandle_t* zhandle_;
	char* host;
	int   timeout;
	unsigned short id_;
	unsigned int time_;
	struct znode_callback_t cb_;
	struct safe_queue_t zevent_;
	unsigned short  watch_node_id_;
	struct znode_watch_path_t* watch_node_;
	struct znode_data_session_t* data_session_;
	unsigned int watch_handle_counter_;
	unsigned int data_handle_counter_;
	char auto_watch_;
	char vaild_;
	//struct spinlock lock;
};

//----------------------------base function-----------------------------------------------------

static unsigned short g_znode_id = 0;
#define ZNODE_NUM 256
static struct znode_t* g_znodes[ZNODE_NUM];

static struct znode_t* get_znode_by_id(unsigned short id) {
	if(id >= ZNODE_NUM)
		return (struct znode_t*)0;
	return g_znodes[id];
}

#define ENCODE_ID(node_id,watch_id) (node_id << 16 | watch_id)
#define DECODE_ID(id)  unsigned short node_id = id >> 16 ; unsigned short watch_id = id & 0x0000ffff

#define ENCODE_SESSION_ID(node_id,session_id) (node_id << 16 | session_id)
#define DECODE_SESSION_ID(id)  unsigned short node_id = id >> 16 ; unsigned short session_id = id & 0x0000ffff

static  unsigned long long g_last_time;

#ifdef _WONDOWS
#include <windows.h>
#else
#include <sys/time.h>
#endif

static unsigned long long get_tick_us() {
#ifdef _WONDOWS
	FILETIME ft;
	ULARGE_INTEGER li;
	GetSystemTimeAsFileTime(&ft);
	li.LowPart  = ft.dwLowDateTime;
	li.HighPart = ft.dwHighDateTime;
	return li.QuadPart / 10;
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (unsigned long long)((unsigned long long) tv.tv_sec * 1000000 + (unsigned long long) tv.tv_usec);
#endif
}

void sleep_ms(unsigned long long ms) 
{
#ifdef _WONDOWS
	Sleep((DWORD)ms);
#else
	usleep(ms * 1000);
#endif
}

unsigned long long get_tick_ms() 
{
	return get_tick_us() / 1000;
}

static char* malloc_and_copy_string(const char* src)
{
	if(!src) return (char*)0;
	int path_len = strlen(src);
	if(path_len > 0){
		char* target  = (char*)zm_malloc(path_len + 1);
		memcpy(target , src, path_len);
		target[path_len] = '\0';
		return target;
	}
	return (char*)0;
}

static void default_stat_completion(int rc, const char *value, int value_len,
	const struct Stat *stat, const void *data);
static int set_watch(struct znode_t* node,const char* path,int id) {
	int ret = zoo_aget(node->zhandle_,path,1,default_stat_completion,(void*)id);
	return ret;
}

static void default_stat_completion_simple(int rc, const struct Stat *stat, const void* data){}
static int set_watch_simple(struct znode_t* node,const char* path,int id) {
	int ret = zoo_aexists(node->zhandle_,path,1,default_stat_completion_simple,(void*)id);
	return ret;
}

static void default_strings_completion(int rc,const struct String_vector *strings,const struct Stat* stat, const void *data);
static int set_watch_child(struct znode_t* node,const char* path,int id){
	int ret = zoo_aget_children2(node->zhandle_,path,1,default_strings_completion,(void*)id);
	return ret;
}

static void default_strings_completion_simple(int rc,const struct String_vector *strings,const struct Stat* stat, const void *data){}
static int set_watch_child_simple(struct znode_t* node,const char* path,int id){
	int ret = zoo_aget_children2(node->zhandle_,path,1,default_strings_completion_simple,(void*)id);
	return ret;
}

char znode_is_vaild(znode_handle* handle)
{
	if(!handle)
		return false;

	struct znode_t* znode = (struct znode_t*)handle;
	if(znode->vaild_!=1)
		return false;

	return true;
}

static struct znode_watch_path_t* get_watch_node(znode_handle* handle, const char* path){
	struct znode_t* znode = (struct znode_t*)handle;
	if (znode->watch_node_)
		return (struct znode_watch_path_t*)0;

	struct znode_watch_path_t* t = (struct znode_watch_path_t *)0;
	HASH_FIND_STR(znode->watch_node_, path, t);
	if (t) {
		return t;
	}
	return (struct znode_watch_path_t*)0;
}


int znode_is_watch_path(znode_handle* handle, const char* path) {
	struct znode_t* znode = (struct znode_t*)handle;
	if (znode->watch_node_)
		return 0;

	struct znode_watch_path_t* t = (struct znode_watch_path_t *)0;
	HASH_FIND_STR(znode->watch_node_, path, t);
	if (t) {
		return 1;
	}
	return 0;
}

int znode_is_watch_path_by_substr(znode_handle* handle, const char* path){
	struct znode_t* znode = (struct znode_t*)handle;
	if (znode->watch_node_)
		return 0;
		
	struct znode_watch_path_t *t, *tmp;
	HASH_ITER(hh, znode->watch_node_, t, tmp) {
		if(t && t->is_watch_child)
		{
			char* p = strstr((char*)path,t->path);
			if(p)
			{
				return 1;
			}
		}
	}
	return 0;
}

static void znode_data_session_free(struct znode_data_session_t* session) {
	if(!session)return;
	if(session->path_){
		zm_free(session->path_);
	}
	zm_free(session);
}

static struct znode_data_session_t* znode_find_data_session(struct znode_t* znode,unsigned short session) {
	int _session = (int)session;
	struct znode_data_session_t* s = (struct znode_data_session_t *)0;
	HASH_FIND_INT(znode->data_session_, &_session, s);
	return s;
}

static void znode_befor_event_free(struct znode_event_t* node){
	if(node->info_type == info_type_data && node->info.data_info.use_session_ == 1){
		//remove session
		struct znode_t* znode = (struct znode_t*)node->info.data_info.znode_;
		struct znode_data_session_t* s = znode_find_data_session(znode,node->info.data_info.session_);
		if (s) {
			HASH_DEL(znode->data_session_, s);
			znode_data_session_free(s);
		}
	}
}

static void znode_event_free(struct znode_event_t* node) {
	if(node->info_type == info_type_event){
		if (node->info.event_info.path_) {
		zm_free(node->info.event_info.path_);
	}
	}else if(node->info_type == info_type_data){
		if(node->info.data_info.path_){
			zm_free(node->info.data_info.path_);
		}
		if (node->info.data_info.data_.value){
			zm_free(node->info.data_info.data_.value);
		}
		if (node->info.data_info.strings_.data) {
			int i;
			for (i = 0; i < node->info.data_info.strings_.count; ++i) {
				zm_free(node->info.data_info.strings_.data[i]);
			}
			zm_free(node->info.data_info.strings_.data);
		}
	}
	zm_free(node);
}

static struct znode_data_session_t* znode_data_session_create(unsigned short session,int op_type,const char* path,struct znode_t* znode) {
	struct znode_data_session_t* data_session = (struct znode_data_session_t*)zm_malloc(sizeof(struct znode_data_session_t));
	data_session->session_ = (int)session;
	data_session->op_type_ = op_type;
	data_session->znode_ = znode;
	data_session->time_ = (unsigned int)get_tick_ms();
	data_session->path_ = malloc_and_copy_string(path);
	HASH_ADD_INT(znode->data_session_, session_, data_session);
	return data_session;
}

static struct znode_event_t* znode_data_create(unsigned short session,char use_session) {
	struct znode_event_t* data = (struct znode_event_t*)zm_malloc(sizeof(struct znode_event_t));
	node_init(&data->node);
	data->info_type = info_type_data;
	memset(&data->info, 0x00, sizeof(struct znode_data_info_t));
	data->info.data_info.session_ = session;
	data->info.data_info.op_type_ = ZNODE_OP_NONE;
	data->info.data_info.znode_ = (void*)0;
	data->info.data_info.path_ = (char*)0;
	data->info.data_info.use_session_ = use_session;
	return data;
}

static bool copy_session_data(struct znode_data_session_t* session,znode_data_info_t* data){
	unsigned int session_id = (unsigned int)data->session_;
	if(session_id != session->session_ || data->znode_ != session->znode_)
	{
		return false;
	}
	data->op_type_ = session->op_type_;
	data->path_ = malloc_and_copy_string(session->path_);
	return true;
}

static void znode_watch_free(struct znode_watch_path_t* node) {
	if(!node)return;
	node->id = 0;
	if (node->path) {
		zm_free(node->path);
	}
	zm_free(node);
}

static void znode_watcher_cb(zhandle_t* zh, int type, int state, const char* path, void* context) {
	struct znode_t* znode = (struct znode_t*)context;
	if(!znode || znode->cb_.on_watch_ == 0){
		return;
	}
	struct znode_event_t* event = (struct znode_event_t*)zm_malloc(sizeof(struct znode_event_t));
	node_init(&event->node);
	event->info_type = info_type_event;
	memset(&event->info, 0x00, sizeof(struct znode_data_info_t));
	event->info.event_info.type_ = type;
	event->info.event_info.state_ = state;
	event->info.event_info.path_ = (char*)0;
	event->info.event_info.path_ = malloc_and_copy_string(path);
	//printf("\n zevent ,type:%d,state:%d,path:%s",type,state,path);
	safe_queue_push_back(&znode->zevent_,&event->node);
}

//-------------------------znode system -----------------------------------------------

void zinit() {
	g_znode_id = 1;
	for(unsigned short i = 0;i < ZNODE_NUM;++i){
		if(g_znodes[i])
			zm_free(g_znodes[i]);
		g_znodes[i] = (struct znode_t*) 0;
	}
	g_last_time = get_tick_ms();
	zm_init();
}

void zuninit() {
	zm_uninit();
	g_znode_id = 0;
	for(unsigned short i = 0;i < ZNODE_NUM;++i){
		g_znodes[i] = (struct znode_t*) 0;
	}
}

void znode_set_debug_level(int level) {
	zoo_set_debug_level((ZooLogLevel)level);
}

static void znode_free(znode_handle* zhandle) {
	struct znode_t* znode = (struct znode_t*)zhandle;
	//SPIN_LOCK(znode);
	if(znode->id_ < ZNODE_NUM)
		g_znodes[znode->id_] = (struct znode_t*)0;
	//SPIN_UNLOCK(znode);
	safe_queue_uninit(&znode->zevent_);
	if(znode->host){
		zm_free(znode->host);
	}
	if (znode->watch_node_)
	{
		struct znode_watch_path_t *t, *tmp;
		HASH_ITER(hh, znode->watch_node_, t, tmp) {
			znode_watch_free(t);
		}
		znode->watch_node_ = (struct znode_watch_path_t*)0;
	}
	if(znode->data_session_)
	{
		struct znode_data_session_t *t, *tmp;
		HASH_ITER(hh, znode->data_session_, t, tmp) {
			znode_data_session_free(t);
		}
		znode->data_session_ = (struct znode_data_session_t*)0;
	}
	zode->vaild_ = 0;
	//SPIN_DESTROY(znode);
	//zm_free(znode);
}

static void _zevent_safe_free(void* p){
	znode_befor_event_free((struct znode_event_t*)p);
	znode_event_free((struct znode_event_t*)p);
}

znode_handle* znode_open(const char* host, int timeout, struct znode_callback_t* cb) {
	struct znode_t* znode = (struct znode_t*)0;
	if(g_znode_id >= ZNODE_NUM)
	{
		g_znode_id = 0;
		return (znode_handle*)znode;  //error!!!
	}
	znode = (struct znode_t*)zm_malloc(sizeof(struct znode_t));
	if (cb)
		znode->cb_ = *cb;
	else
		memset(&znode->cb_, 0x00, sizeof(struct znode_callback_t));
	safe_queue_init(&znode->zevent_);
	znode->zevent_.free_ = _zevent_safe_free;
	znode->watch_node_id_ = 1;

	znode->time_ = 0;
	znode->watch_node_ = (struct znode_watch_path_t*)0;
	znode->data_session_ = (struct znode_data_session_t*)0;
	znode->host = malloc_and_copy_string(host);
	znode->timeout = timeout;
	znode->zhandle_ = zookeeper_init(host, znode_watcher_cb, timeout, 0, znode, 0);
	znode->watch_handle_counter_ = 0;
	znode->data_handle_counter_ = 0;
	znode->auto_watch_ = 1;
	if (!znode->zhandle_) {
		znode_free(znode);
		return (znode_handle*)0;
	}
	znode->id_ = g_znode_id++;
	g_znodes[znode->id_] = znode;
	znode->vaild_ = 1;
	//SPIN_INIT(znode);
	return (znode_handle*)znode;
}

void znode_set_auto_watch(znode_handle* handle,char enable)
{
	if(!znode_is_vaild(handle)) return;
	struct znode_t* znode = (struct znode_t*)handle;
	znode->auto_watch_ = enable;
}

void znode_close(znode_handle* handle) {
	struct znode_t* znode = (struct znode_t*)handle;
	if(znode->zhandle_)
		zookeeper_close(znode->zhandle_);
	znode_free(znode);
}

void znode_add_watch_path(znode_handle* handle, const char* path, int is_watch_child){
	if(!znode_is_vaild(handle)) return;
	struct znode_t* znode = (struct znode_t*)handle;
	if (znode_is_watch_path(znode, path))
		return;
	struct znode_watch_path_t* node = (struct znode_watch_path_t*)zm_malloc(sizeof(struct znode_watch_path_t));
	node->is_watch_child = is_watch_child;
	node->id = ++znode->watch_node_id_;
	node->is_watched = 0;
	node->path = malloc_and_copy_string(path);
	HASH_ADD_STR(znode->watch_node_, path, node);
	if(is_watch_child){
		set_watch_child(znode,path,ENCODE_ID(znode->id_,node->id));
	}else{
		set_watch(znode,path,ENCODE_ID(znode->id_,node->id));
	}
}

void znode_remove_watch_path(znode_handle* handle, const char* path) {
	struct znode_t* znode = (struct znode_t*)handle;
	if (!znode->watch_node_)
		return;

	struct znode_watch_path_t* t = (struct znode_watch_path_t *)0;
	HASH_FIND_STR(znode->watch_node_, path, t);
	if (t) {
		HASH_DEL(znode->watch_node_, t);
		znode_watch_free(t);
	}
}

static void _znode_update(struct znode_t* znode) {
	for(;;)
	{
		struct znode_event_t* ev = (struct znode_event_t*)safe_queue_pop_front(&znode->zevent_);
		if (!ev) {
			break;
		}
		if(znode->vaild_!=1)
		{
			znode_befor_event_free(ev);
			znode_event_free(ev);
			continue;
		}
		switch(ev->info_type)
		{
			case info_type_event:
			{	
				int   type = ev->info.event_info.type_;
				int   state = ev->info.event_info.state_;
				char* path = ev->info.event_info.path_;

				znode->watch_handle_counter_ += 1;
				//printf("\r\n znode id:%d,handle watch event : %d,type:%d,path:%s",znode->id_,znode->watch_handle_counter_,type,path);

				if(type == ZOO_CREATED_EVENT
					|| type == ZOO_DELETED_EVENT
					|| type == ZOO_CHANGED_EVENT){

					//watch once
					if(znode->auto_watch_)
					{
						struct znode_watch_path_t* watch_node = get_watch_node(znode, path);
						if (watch_node){
							set_watch_simple(znode, path,ENCODE_ID(znode->id_,watch_node->id));
						}
					}
					//printf("\r\n watch node type\r\n");
				}
				else if(type == ZOO_CHILD_EVENT)
				{
					if(znode->auto_watch_)
					{
						struct znode_watch_path_t* watch_node = get_watch_node(znode, path);
						if (watch_node){
							set_watch_child_simple(znode, path,ENCODE_ID(znode->id_,watch_node->id));
						}
					}
					//printf("\r\n child event \r\n");
				}
				else if(type == ZOO_SESSION_EVENT)
				{
					if(state == ZOO_CONNECTED_STATE) {
						zoo_set_watcher(znode->zhandle_,znode_watcher_cb);
						if (znode->watch_node_)
						{
							struct znode_watch_path_t *t, *tmp;
							HASH_ITER(hh, znode->watch_node_, t, tmp) 
							{
								if(t)
								{
									t->is_watched = 0;
									if (t->is_watch_child)
										set_watch_child(znode,t->path,ENCODE_ID(znode->id_,t->id));
									else
										set_watch(znode,t->path,ENCODE_ID(znode->id_,t->id));
								}
							}
						}
					}
					else if(state == ZOO_EXPIRED_SESSION_STATE)
					{
						//reconnect!
						if(znode->zhandle_) {
							zookeeper_close(znode->zhandle_);
							znode->zhandle_ = zookeeper_init(znode->host, znode_watcher_cb, znode->timeout, 0, znode, 0);
						}
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
				if (znode->cb_.on_watch_) {
					znode->cb_.on_watch_(znode, & (ev->info.event_info) );
				}
			}
			break;

			case info_type_data:
			{
				struct znode_data_info_t* info = &(ev->info.data_info);
				char vaild = 1;
				info->znode_ = get_znode_by_id(info->znode_id_);
				if(znode != info->znode_)
				{
					vaild = 0;
				}
				if(vaild == 1)
				{
					if(info->use_session_ == 1)
					{
						struct znode_data_session_t* data_session = znode_find_data_session(znode,info->session_);
						if(data_session)
						{
							if(!copy_session_data(data_session,info))
							{
								vaild = 0;
							}	
						}
						else
						{
							vaild = 0;
						}
					}

					if(info->op_type_ == ZNODE_OP_SET_WATCH 
						|| info->op_type_ == ZNODE_OP_SET_WATCH_CHILDREN)
					{
						struct znode_watch_path_t *t, *tmp;
						HASH_ITER(hh, znode->watch_node_, t, tmp) 
						{
							if(t && t->id == info->watch_id_)
							{
								if(info->rc_ == ZOK)
								{
									t->is_watched = 1;
								}
								info->path_ = malloc_and_copy_string(t->path);
							}
						}
					}

					znode->data_handle_counter_ += 1;
					//printf("\r\n znode id:%d,handle data event : %d,op_type:%d,path:%s",znode->id_,znode->data_handle_counter_,info->op_type_,info->path_);

					if (znode->cb_.on_async_data_) 
					{
						znode->cb_.on_async_data_(znode, &(ev->info.data_info) );
					}
				}
			}
			break;
		}
		znode_befor_event_free(ev);
		znode_event_free(ev);
	}
}

void znode_update() 
{
	unsigned long long time_now = get_tick_ms();
	unsigned short ms = time_now - g_last_time;
	g_last_time = time_now;

	for(unsigned short i = 0;i < ZNODE_NUM;++i)
	{
		struct znode_t* znode = g_znodes[i];
		if(!znode)
			continue;

		_znode_update(znode);

		if(znode->vaild_==1)
		{
			znode->time_ += ms;
			if( (znode->timeout / 2 ) < znode->time_)
			{
				//remove timeout session
				{
					struct znode_data_session_t* t,*tmp;
					HASH_ITER(hh, znode->data_session_, t, tmp)
					{
						if(t && time_now - t->time_ > znode->timeout)
						{
							HASH_DEL(znode->data_session_, t);
							znode_data_session_free(t);
						}
					} 
				}

				//auto retry watch
				{
					znode->time_ = 0;
					struct znode_watch_path_t *t, *tmp;
					HASH_ITER(hh, znode->watch_node_, t, tmp) 
					{
						if(t && t->is_watched == 0)
						{
							//printf("\n watch_retry znode id %d ,name:%s",znode->id_,t->path);
							if (t->is_watch_child)
								set_watch_child(znode,t->path,ENCODE_ID(znode->id_,t->id));
							else
								set_watch(znode,t->path,ENCODE_ID(znode->id_,t->id));
						}
					}
				}
			}
		}
	}
}

//------------sync api----------------------------------------------------------------------------------

int znode_create(znode_handle* handle, const char* path, const char* value, int value_len, int flags) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_create(znode->zhandle_, path, value, value_len,
		&ZOO_OPEN_ACL_UNSAFE, flags, (char*)0,0);
	return rc;
}

int znode_exists(znode_handle* handle, const char* path) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_exists(znode->zhandle_, path, 1, NULL);
	return rc;
}

int znode_delete(znode_handle* handle, const char* path, int version) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_delete(znode->zhandle_, path, version);
	return rc;
}

int znode_get(znode_handle* handle, const char* path, char* buffer, int* buffer_len, int* version) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	struct Stat stat;
	stat.version = 0;
	int rc = zoo_get(znode->zhandle_, path, 1, buffer, buffer_len, &stat);
	*version = stat.version;
	return rc;
}

int znode_set(znode_handle* handle, const char* path, const char* buffer, int buffer_len, int version) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_set(znode->zhandle_, path, buffer, buffer_len, version);
	return rc;
}

int znode_get_children(znode_handle* handle, const char* path, int* count, char** child_paths, int* version) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	struct String_vector strings;
	struct Stat stat;
	strings.count = 0;
	stat.version = 0;
	int ret = zoo_get_children2(znode->zhandle_, path, 1, &strings, &stat);
	*version = stat.version;
	*count = strings.count;
	int i;
	for (i = 0; i<strings.count; ++i)
	{
		child_paths[i] = strings.data[i];
	}
	return ret;
}

//------------------async callback---------------------------------------------------------------------


static void default_stat_completion(int rc, const char *value, int value_len,
	const struct Stat *stat, const void *data) {
	long id = (long)(data);
	id &= 0xffffffff;
	DECODE_ID(id);
	struct znode_t* znode = get_znode_by_id(node_id);
	if(!znode)
	{
		printf("\n default_stat_completion znode id error %d",node_id);
		return;
	}
	if(znode->vaild_!=1)return;
	struct znode_event_t* d = znode_data_create(0,0);
	struct znode_data_info_t* info = &((d->info).data_info);
	info->rc_ = rc;
	info->op_type_ = ZNODE_OP_SET_WATCH;
	info->watch_id_ = watch_id;
	info->znode_id_ = node_id;
	if (stat)
		info->version_ = stat->version;
	if (value_len > 0){
		info->data_.value_len = value_len;
		info->data_.value = (char*)zm_malloc(value_len+1);
		memcpy(info->data_.value, value, value_len);
		info->data_.value[value_len] = '\0';
	}
	safe_queue_push_back(&znode->zevent_, &(d->node));
}

static void default_strings_completion(int rc,const struct String_vector *strings,const struct Stat* stat, const void *data) {
	long id = (long)(data);
	id &= 0xffffffff;
	DECODE_ID(id);
	struct znode_t* znode = get_znode_by_id(node_id);
	if(!znode)
	{
		printf("\n default_strings_completion znode id error %d",node_id);
		return;
	}
	if(znode->vaild_!=1)return;
	struct znode_event_t* d = znode_data_create(0,0);
	struct znode_data_info_t* info = &((d->info).data_info);
	info->rc_ = rc;
	info->op_type_ = ZNODE_OP_SET_WATCH_CHILDREN;
	info->watch_id_ = watch_id;
	info->znode_id_ = node_id;
	if (stat)
		info->version_ = stat->version;
	if(rc == ZOK)
	{
		//printf("\n default_strings_completion watch id  %d ,is watched",watch_id);
		if (strings && strings->count > 0) {
			info->strings_.count = strings->count;
			info->strings_.data = (char**)zm_malloc(sizeof(char*)*strings->count);
			int i;
			for (i = 0; i<strings->count; ++i)
			{
				info->strings_.data[i] = malloc_and_copy_string(strings->data[i]);
			}
		}
	}
	//printf("\n default_strings_completion watch id  %d,is faild!",watch_id);
	safe_queue_push_back(&znode->zevent_, &(d->node));
}

#define ASYNC_DATA_HANDLE(data) \
unsigned long param_id = (unsigned long)(data);\
param_id &= 0xffffffff;\
DECODE_SESSION_ID(param_id);\
struct znode_t* znode = get_znode_by_id(node_id);\
if(!znode)\
{\
	printf("\n ASYNC_DATA_HANDLE znode id error %d",node_id);\
	return;\
}\
if(znode->vaild_!=1)return;\
struct znode_event_t* d = znode_data_create(session_id,1);\
struct znode_data_info_t* info = &((d->info).data_info);\
info->znode_id_ = node_id;

static void znode_void_completion_cb(int rc, const void *data) {
	ASYNC_DATA_HANDLE(data);
	info->rc_ = rc;
	//printf("\n znode_void_completion_cb,op_type:%d,path:%s",info->op_type_,info->path_);
	safe_queue_push_back(&znode->zevent_, &(d->node));
}

static void znode_data_completion_cb(int rc, const char *value, int value_len,
	const struct Stat *stat, const void *data) {
	ASYNC_DATA_HANDLE(data);
	info->rc_ = rc;
	if (stat)
		info->version_ = stat->version;
	if (value_len > 0){
		info->data_.value_len = value_len;
		info->data_.value = (char*)zm_malloc(value_len+1);
		memcpy(info->data_.value, value, value_len);
		info->data_.value[value_len] = '\0';
	}
	//printf("\n znode_data_completion_cb,op_type:%d,path:%s",info->op_type_,info->path_);
	safe_queue_push_back(&znode->zevent_, &(d->node));
}

static void znode_stat_completion_cb(int rc, const struct Stat *stat, const void *data) {
	ASYNC_DATA_HANDLE(data);
	info->rc_ = rc;
	if(stat)
		info->version_ = stat->version;
	//printf("\n znode_stat_completion_cb,op_type:%d,path:%s",info->op_type_,info->path_);
	safe_queue_push_back(&znode->zevent_, &(d->node));
}

static void znode_aset_stat_completion_cb(int rc, const struct Stat *stat, const void *data) {
	ASYNC_DATA_HANDLE(data);
	info->rc_ = rc;
	if(stat)
		info->version_ = stat->version;
	if(rc == ZOK)
	{
		info->rc_ = zoo_aget(znode->zhandle_, d->info.data_info.path_, 1, znode_data_completion_cb, data);
	}
	else
	{
		safe_queue_push_back(&znode->zevent_, &(d->node));
	}
}

static void znode_string_completion_cb(int rc, const char *value, const void *data) {
	ASYNC_DATA_HANDLE(data);
	info->rc_ = rc;
	int value_len = 0;
	if(value){
		value_len = strlen(value); 
	}
	info->data_.value_len = value_len;
	if(value_len > 0){
		info->data_.value = (char*)zm_malloc(value_len + 1);
		memcpy(info->data_.value, value, value_len);
		info->data_.value[value_len] = '\0';
	}
	//printf("\n znode_string_completion_cb,op_type:%d,path:%s",info->op_type_,info->path_);
	safe_queue_push_back(&znode->zevent_, &(d->node));
}

static void znode_strings_completion_cb(int rc,
	const struct String_vector *strings, const void *data) {
	ASYNC_DATA_HANDLE(data);
	info->rc_ = rc;
	if (strings && strings->count > 0){
		info->strings_.count = strings->count;
		info->strings_.data = (char**)zm_malloc(sizeof(char*)*strings->count);
		int i;
		for (i = 0; i<strings->count; ++i)
		{
			info->strings_.data[i] = malloc_and_copy_string(strings->data[i]);
		}
	}
	//printf("\n znode_strings_completion_cb,op_type:%d,path:%s",info->op_type_,info->path_);
	safe_queue_push_back(&znode->zevent_,&(d->node));
}

static void znode_strings_stat_completion_cb(int rc,
	const struct String_vector *strings, const struct Stat* stat, const void *data) {
	ASYNC_DATA_HANDLE(data);
	info->rc_ = rc;
	if (stat)
	{
		info->version_ = stat->version;
	}
	if (strings && strings->count > 0) {
		info->strings_.count = strings->count;
		info->strings_.data = (char**)zm_malloc(sizeof(char*)*strings->count);
		int i;
		for (i = 0; i<strings->count; ++i)
		{
			info->strings_.data[i] = malloc_and_copy_string(strings->data[i]);
		}
	}
	//printf("\n znode_strings_stat_completion_cb,op_type:%d,path:%s",info->op_type_,info->path_);
	safe_queue_push_back(&znode->zevent_, &(d->node));
}

//------------------------async api-----------------------------------------------------------------------

int znode_acreate(znode_handle* handle, unsigned short session, const char* path, const char* value, int value_len, int flags) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	znode_data_session_create(session,ZNODE_OP_CREATE,path,znode);
	unsigned int param_id = ENCODE_SESSION_ID(znode->id_,session); 
	int rc = zoo_acreate(znode->zhandle_, path, value, value_len,
		&ZOO_OPEN_ACL_UNSAFE, flags, znode_string_completion_cb, (const void*)param_id);
	return rc;
}

int znode_adelete(znode_handle* handle, unsigned short session, const char* path, int version) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	znode_data_session_create(session,ZNODE_OP_DELETE,path,znode);
	unsigned int param_id = ENCODE_SESSION_ID(znode->id_,session); 
	int ret = zoo_adelete(znode->zhandle_, path, version, znode_void_completion_cb, (const void*)param_id);
	return ret;
}

int znode_aexists(znode_handle* handle, unsigned short session, const char* path) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	znode_data_session_create(session,ZNODE_OP_EXISTS,path,znode);
	unsigned int param_id = ENCODE_SESSION_ID(znode->id_,session); 
	int ret = zoo_aexists(znode->zhandle_, path, 1, znode_stat_completion_cb, (const void*)param_id);
	return ret;
}

int znode_aget(znode_handle* handle, unsigned short session, const char* path) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	znode_data_session_create(session,ZNODE_OP_GET,path,znode);
	unsigned int param_id = ENCODE_SESSION_ID(znode->id_,session); 
	int ret = zoo_aget(znode->zhandle_, path, 1, znode_data_completion_cb, (const void*)param_id);
	return ret;
}

int znode_aset(znode_handle* handle, unsigned short session, const char* path, const char* buffer, int buffer_len, int version) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	znode_data_session_create(session,ZNODE_OP_SET,path,znode);
	unsigned int param_id = ENCODE_SESSION_ID(znode->id_,session); 
	int ret = zoo_aset(znode->zhandle_, path, buffer, buffer_len, version, znode_aset_stat_completion_cb, (const void*)param_id);
	return ret;
}

int znode_aget_children(znode_handle* handle, unsigned short session, const char* path) {
	if(!znode_is_vaild(handle)) return -1;
	struct znode_t* znode = (struct znode_t*)handle;
	znode_data_session_create(session,ZNODE_OP_GET_CHILDREN,path,znode);
	unsigned int param_id = ENCODE_SESSION_ID(znode->id_,session); 
	int ret = zoo_aget_children2(znode->zhandle_, path, 1, znode_strings_stat_completion_cb, (const void*)param_id);
	return ret;
}
