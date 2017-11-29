

#include "znode.h"
#include "linked.h"
#include "safe_queue.h"
#include "smemory.h"
#include"uthash.h"
#include <stdio.h>

#include <zookeeper.h>
#include <zookeeper_log.h>



struct znode_event_t {
	struct node_t node;
	struct znode_event_info_t info;
};

struct znode_data_t {
	struct node_t node;
	struct znode_data_info_t info;
};

struct znode_watch_path_t {
	char* path;
	int   is_watch_child;
	UT_hash_handle hh;
};

struct znode_t{
	zhandle_t* zhandle_;
	char* host;
	int   timeout;

	struct znode_callback_t cb_;
	struct safe_queue_t zevent_;
	struct safe_queue_t zdata_;
	struct znode_watch_path_t* watch_node_;
};

static void
default_stat_completion(int rc, const struct Stat *stat, const void *data) {

}

static int 
set_watch(struct znode_t* node,const char* path){
	int ret = zoo_aexists(node->zhandle_,path,1,default_stat_completion,NULL);
	return ret;
}

static void
default_strings_completion(int rc,const struct String_vector *strings, const void *data) {
}

static int 
set_watch_child(struct znode_t* node,const char* path){
	int ret = zoo_aget_children(node->zhandle_,path,1,default_strings_completion,NULL);
	return ret;
}

int
znode_is_watch_path(znode_handle* handle, char* path) {
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

static void 
znode_event_free(struct znode_event_t* node) {
	if (node->info.path_) {
		zm_free(node->info.path_);
	}
	zm_free(node);
}

static void 
znode_data_free(struct znode_data_t* node) {
	if (node->info.data_.value){
		zm_free(node->info.data_.value);
	}
	if (node->info.strings_.data) {
		for (int i = 0; i < node->info.strings_.count; ++i) {
			zm_free(node->info.strings_.data[i]);
		}
		zm_free(node->info.strings_.data);
	}
	zm_free(node);
}

static struct znode_data_t*
znode_data_create(int session,int op_type,char* path,struct znode_t* znode) {
	struct znode_data_t* data = (struct znode_data_t*)zm_malloc(sizeof(struct znode_data_t));
	node_init(&data->node);
	memset(&data->info, 0x00, sizeof(struct znode_data_info_t));
	data->info.session_ = session;
	data->info.op_type_ = op_type;
	data->info.znode_ = znode;
	int path_len = strlen(path);
	data->info.path_ = zm_malloc(path_len + 1);
	memcpy(data->info.path_, path, path_len);
	data->info.path_[path_len] = '\0';
	return data;
}

static void
znode_watch_free(struct znode_watch_path_t* node) {
	if (node->path) {
		zm_free(node->path);
	}
	zm_free(node);
}

static void
znode_watcher_cb(zhandle_t* zh, int type, int state, const char* path, void* context) {
	struct znode_t* znode = (struct znode_t*)context;
	if(!znode || znode->cb_.on_watch_ == 0){
		return;
	}
	struct znode_event_t* event = (struct znode_event_t*)zm_malloc(sizeof(struct znode_event_t));
	node_init(&event->node);
	event->info.type_ = type;
	event->info.state_ = state;
	event->info.path_ = (char*)0;
	if(path){
		int path_len = strlen(path);
		event->info.path_ = (char*)zm_malloc(path_len+1);
		memcpy(event->info.path_,path,path_len);
		event->info.path_[path_len] = '\0';
	}
	safe_queue_push_back(&znode->zevent_,&event->node);
}

//-----------------------------------------------------------------------------------

void
zinit() {
	zm_init();
}

void
zuninit() {
	zm_uninit();
}

void 
znode_set_debug_level(int level) {
	zoo_set_debug_level(level);
}

static void
znode_free(znode_handle* zhandle) {
	struct znode_t* znode = (struct znode_t*)zhandle;
	safe_queue_uninit(&znode->zevent_);
	safe_queue_uninit(&znode->zdata_);
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
	zm_free(znode);
}

static void 
_zevent_safe_free(void* p){
	znode_event_free((struct znode_event_t*)p);
}

static void
_zdata_safe_free(void* p) {
	znode_data_free((struct znode_data_t*)p);
}

znode_handle* 
znode_open(char* host, int timeout,struct znode_callback_t* cb) {
	struct znode_t* znode = (struct znode_t*)zm_malloc(sizeof(struct znode_t));
	if (cb)
		znode->cb_ = *cb;
	else
		memset(&znode->cb_, 0x00, sizeof(struct znode_callback_t));
	safe_queue_init(&znode->zevent_);
	znode->zevent_.free_ = _zevent_safe_free;
	safe_queue_init(&znode->zdata_);
	znode->zdata_.free_ = _zdata_safe_free;
	znode->watch_node_ = (struct znode_watch_path_t*)0;
	int host_len = strlen(host);
	znode->host = zm_malloc(host_len+1);
	memcpy(znode->host,host,host_len);
	znode->host[host_len] = '\0';
	znode->timeout = timeout;
	znode->zhandle_ = zookeeper_init(host, znode_watcher_cb, timeout, 0, znode, 0);
	if (!znode->zhandle_) {
		znode_free(znode);
		return (znode_handle*)0;
	}
	return (znode_handle*)znode;
}

void 
znode_close(znode_handle* handle) {
	struct znode_t* znode = (struct znode_t*)handle;
	if(znode->zhandle_)
		zookeeper_close(znode->zhandle_);
	znode_free(znode);
}


void
znode_add_watch_path(znode_handle* handle,char* path,int is_watch_child){
	struct znode_t* znode = (struct znode_t*)handle;
	if (znode_is_watch_path(znode, path))
		return;
	struct znode_watch_path_t* node = (struct znode_watch_path_t*)zm_malloc(sizeof(struct znode_watch_path_t));
	node->is_watch_child = is_watch_child;
	int path_len = strlen(path);
	node->path = zm_malloc(path_len + 1);
	memcpy(node->path, path, path_len);
	node->path[path_len] = '\0';
	HASH_ADD_STR(znode->watch_node_, path, node);
	if(is_watch_child){
		set_watch_child(znode,path);
	}else{
		set_watch(znode,path);
	}
}

void
znode_remove_watch_path(znode_handle* handle, char* path) {
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


void
znode_update(znode_handle* zhandle) {
	struct znode_t* znode = (struct znode_t*)zhandle;
	for(;;){
		struct znode_event_t* ev = (struct znode_event_t*)safe_queue_pop_front(&znode->zevent_);
		if (!ev) {
			break;
		}
		int   type = ev->info.type_;
		int   state = ev->info.state_;
		char* path = ev->info.path_;
		if(type == ZOO_CREATED_EVENT
			|| type == ZOO_DELETED_EVENT
			|| type == ZOO_CHANGED_EVENT){
			if (znode_is_watch_path(znode, path) == 1){
				int ret = set_watch(znode, path);
			}
		}
		else if(type == ZOO_CHILD_EVENT)
		{
			if (znode_is_watch_path(znode, path) == 1) {
				int ret = set_watch_child(znode, path);
			}
		}
		else if(type == ZOO_SESSION_EVENT)
		{
			if(state == ZOO_CONNECTED_STATE) {
				zoo_set_watcher(znode->zhandle_,znode_watcher_cb);
				if (znode->watch_node_)
				{
					struct znode_watch_path_t *t, *tmp;
					HASH_ITER(hh, znode->watch_node_, t, tmp) {
						if (t->is_watch_child)
							set_watch_child(znode, t->path);
						else
							set_watch(znode, t->path);
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
			znode->cb_.on_watch_(znode, &ev->info);
		}
		znode_event_free(ev);
	}
	for (;;) {
		struct znode_data_t* data = (struct znode_data_t*)safe_queue_pop_front(&znode->zdata_);
		if (!data) {
			break;
		}
		if (znode->cb_.on_async_data_) {
			znode->cb_.on_async_data_(znode, &data->info);
		}
		znode_data_free(data);
	}
}


//------------sync api----------------------------------

int
znode_create(znode_handle* handle, char* path, char* value, int value_len, int flags) {
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_create(znode->zhandle_, path, value, value_len,
		&ZOO_OPEN_ACL_UNSAFE, flags, (char*)0,0);
	return rc;
}

int
znode_exists(znode_handle* handle,char* path) {
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_exists(znode->zhandle_, path, 1, NULL);
	return rc;
}

int
znode_delete(znode_handle* handle, char* path,int version) {
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_delete(znode->zhandle_, path, version);
	return rc;
}

int
znode_get(znode_handle* handle,char* path,char* buffer,int* buffer_len,int* version) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct Stat stat;
	stat.version = 0;
	int rc = zoo_get(znode->zhandle_, path, 1, buffer, buffer_len, &stat);
	*version = stat.version;
	return rc;
}

int
znode_set(znode_handle* handle, char* path, char* buffer, int buffer_len,int version) {
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_set(znode->zhandle_, path, buffer, buffer_len, version);
	return rc;
}

int
znode_get_children(znode_handle* handle,char* path,int* count,char** child_paths,int* version) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct String_vector strings;
	struct Stat stat;
	strings.count = 0;
	stat.version = 0;
	int ret = zoo_get_children2(znode->zhandle_, path, 1, &strings, &stat);
	*version = stat.version;
	for (int i = 0; i<strings.count; ++i)
	{
		child_paths[i] = strings.data[i];
	}
	return ret;
}


//------------------async api---------------------------------

static void
znode_void_completion_cb(int rc, const void *data) {
	if (data == NULL) return;
	struct znode_data_t* d = (struct znode_data_t*)data;
	struct znode_t* znode = (struct znode_t*)(d->info).znode_;
	d->info.rc_ = rc;
	safe_queue_push_back(&znode->zdata_, &(d->node));
}

static void
znode_stat_completion_cb(int rc, const struct Stat *stat, const void *data) {
	if (data == NULL) return;
	struct znode_data_t* d = (struct znode_data_t*)data;
	struct znode_t* znode = (struct znode_t*)(d->info).znode_;
	d->info.rc_ = rc;
	if(stat)
		d->info.version_ = stat->version;
	safe_queue_push_back(&znode->zdata_, &(d->node));
}

static void
znode_data_completion_cb(int rc, const char *value, int value_len,
	const struct Stat *stat, const void *data) {
	if (data == NULL) return;
	struct znode_data_t* d = (struct znode_data_t*)data;
	struct znode_t* znode = (struct znode_t*)(d->info).znode_;
	d->info.rc_ = rc;
	if (stat)
		d->info.version_ = stat->version;
	if (value_len > 0){
		(d->info).data_.value_len = value_len;
		(d->info).data_.value = (char*)zm_malloc(value_len+1);
		memcpy((d->info).data_.value, value, value_len);
		(d->info).data_.value[value_len] = '\0';
	}
	safe_queue_push_back(&znode->zdata_, &(d->node));
}

static void
znode_string_completion_cb(int rc, const char *value, const void *data) {
	if (data == NULL) return;
	struct znode_data_t* d = (struct znode_data_t*)data;
	struct znode_t* znode = (struct znode_t*)(d->info).znode_;
	d->info.rc_ = rc;
	int value_len = 0;
	if(value){
		value_len = strlen(value); 
	}
	(d->info).data_.value_len = value_len;
	if(value_len > 0){
		(d->info).data_.value = (char*)zm_malloc(value_len + 1);
		memcpy((d->info).data_.value, value, value_len);
		(d->info).data_.value[value_len] = '\0';
	}
	safe_queue_push_back(&znode->zdata_, &(d->node));
}


static void
znode_strings_completion_cb(int rc,
	const struct String_vector *strings, const void *data) {
	if (data == NULL) return;
	struct znode_data_t* d = (struct znode_data_t*)data;
	struct znode_t* znode = (struct znode_t*)(d->info).znode_;
	d->info.rc_ = rc;
	if (strings->count > 0){
		(d->info).strings_.count = strings->count;
		(d->info).strings_.data = (char**)zm_malloc(sizeof(char*)*strings->count);
		for (int i = 0; i<strings->count; ++i)
		{
			int value_len = strlen(strings->data[i]);
			(d->info).strings_.data[i] = (char*)zm_malloc(value_len + 1);
			memcpy((d->info).strings_.data[i], strings->data[i], value_len);
			(d->info).strings_.data[i][value_len] = '\0';
		}
	}
	safe_queue_push_back(&znode->zdata_,&(d->node));
}


static void
znode_strings_stat_completion_cb(int rc,
	const struct String_vector *strings, const struct Stat* stat, const void *data) {
	if (data == NULL) return;
	struct znode_data_t* d = (struct znode_data_t*)data;
	struct znode_t* znode = (struct znode_t*)(d->info).znode_;
	d->info.rc_ = rc;
	if (stat)
	{
		(d->info).version_ = stat->version;
	}
	if (strings && strings->count > 0) {
		(d->info).strings_.count = strings->count;
		(d->info).strings_.data = (char**)zm_malloc(sizeof(char*)*strings->count);
		for (int i = 0; i<strings->count; ++i)
		{
			int value_len = strlen(strings->data[i]);
			(d->info).strings_.data[i] = (char*)zm_malloc(value_len + 1);
			memcpy((d->info).strings_.data[i], strings->data[i], value_len);
			(d->info).strings_.data[i][value_len] = '\0';
		}
	}
	safe_queue_push_back(&znode->zdata_, &(d->node));
}


int
znode_acreate(znode_handle* handle,int session,char* path,char* value,int value_len,int flags) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_data_t* data = znode_data_create(session, ZNODE_OP_CREATE, path, znode);
	int rc = zoo_acreate(znode->zhandle_, path, value, value_len,
		&ZOO_OPEN_ACL_UNSAFE, flags, znode_string_completion_cb, data);
	return rc;
}

int
znode_adelete(znode_handle* handle, int session, char* path,int version) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_data_t* data = znode_data_create(session, ZNODE_OP_DELETE, path, znode);
	int ret = zoo_adelete(znode->zhandle_, path, version, znode_void_completion_cb, data);
	return ret;
}

int
znode_aexists(znode_handle* handle, int session, char* path) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_data_t* data = znode_data_create(session, ZNODE_OP_EXISTS, path, znode);
	int ret = zoo_aexists(znode->zhandle_, path, 1, znode_stat_completion_cb, data);
	return ret;
}

int
znode_aget(znode_handle* handle, int session, char* path) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_data_t* data = znode_data_create(session, ZNODE_OP_GET, path, znode);
	int ret = zoo_aget(znode->zhandle_, path, 1, znode_data_completion_cb, data);
	return ret;
}

int
znode_aset(znode_handle* handle, int session, char* path,char* buffer,int buffer_len,int version) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_data_t* data = znode_data_create(session, ZNODE_OP_SET, path, znode);
	int ret = zoo_aset(znode->zhandle_, path, buffer, buffer_len, version, znode_data_completion_cb, data);
	return ret;
}

int
znode_aget_children(znode_handle* handle, int session, char* path) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_data_t* data = znode_data_create(session, ZNODE_OP_GET_CHILDREN, path, znode);
	int ret = zoo_aget_children2(znode->zhandle_, path, 1, znode_strings_stat_completion_cb, data);
	return ret;
}
