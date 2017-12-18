

#include "znode.h"
#include "linked.h"
#include "safe_queue.h"
#include "smemory.h"
#include "uthash.h"
#include <stdio.h>

#include <zookeeper.h>
#include <zookeeper_log.h>

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

	struct znode_watch_path_t* watch_node_;
};

static void default_stat_completion(int rc, const struct Stat *stat, const void *data) {

}

static int set_watch(struct znode_t* node,const char* path){
	int ret = zoo_aexists(node->zhandle_,path,1,default_stat_completion,NULL);
	return ret;
}

static void default_strings_completion(int rc,const struct String_vector *strings, const void *data) {
}

static int set_watch_child(struct znode_t* node,const char* path){
	int ret = zoo_aget_children(node->zhandle_,path,1,default_strings_completion,NULL);
	return ret;
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

int  znode_is_watch_path_by_substr(znode_handle* handle, const char* path){
	struct znode_t* znode = (struct znode_t*)handle;
	if (znode->watch_node_)
		return 0;
		
	struct znode_watch_path_t *t, *tmp;
	HASH_ITER(hh, znode->watch_node_, t, tmp) {
		if(tmp->is_watch_child)
		{
			char* p = strstr((char*)path,tmp->path);
			if(p)
			{
				return 1;
			}
		}
	}
	return 0;
}

static void znode_event_free(struct znode_event_t* node) {
	if(node->info_type == info_type_event){
		if (node->info.event_info.path_) {
		zm_free(node->info.event_info.path_);
	}
	}else if(node->info_type == info_type_data){
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


static struct znode_event_t* znode_data_create(int session, int op_type, const char* path, struct znode_t* znode) {
	struct znode_event_t* data = (struct znode_event_t*)zm_malloc(sizeof(struct znode_event_t));
	node_init(&data->node);
	data->info_type = info_type_data;
	memset(&data->info, 0x00, sizeof(struct znode_data_info_t));
	data->info.data_info.session_ = session;
	data->info.data_info.op_type_ = op_type;
	data->info.data_info.znode_ = znode;
	int path_len = strlen(path);
	data->info.data_info.path_ = (char*)zm_malloc(path_len + 1);
	memcpy(data->info.data_info.path_, path, path_len);
	data->info.data_info.path_[path_len] = '\0';
	return data;
}

static void znode_watch_free(struct znode_watch_path_t* node) {
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
	if(path){
		int path_len = strlen(path);
		event->info.event_info.path_ = (char*)zm_malloc(path_len+1);
		memcpy(event->info.event_info.path_,path,path_len);
		event->info.event_info.path_[path_len] = '\0';
	}

	//printf("\n zevent ,type:%d,state:%d,path:%s",type,state,path);
	safe_queue_push_back(&znode->zevent_,&event->node);
}

//-----------------------------------------------------------------------------------

void zinit() {
	zm_init();
}

void zuninit() {
	zm_uninit();
}

void znode_set_debug_level(int level) {
	zoo_set_debug_level((ZooLogLevel)level);
}

static void znode_free(znode_handle* zhandle) {
	struct znode_t* znode = (struct znode_t*)zhandle;
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
	zm_free(znode);
}

static void _zevent_safe_free(void* p){
	znode_event_free((struct znode_event_t*)p);
}



znode_handle* znode_open(const char* host, int timeout, struct znode_callback_t* cb) {
	struct znode_t* znode = (struct znode_t*)zm_malloc(sizeof(struct znode_t));
	if (cb)
		znode->cb_ = *cb;
	else
		memset(&znode->cb_, 0x00, sizeof(struct znode_callback_t));
	safe_queue_init(&znode->zevent_);
	znode->zevent_.free_ = _zevent_safe_free;
	znode->watch_node_ = (struct znode_watch_path_t*)0;
	int host_len = strlen(host);
	znode->host = (char*)zm_malloc(host_len+1);
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

void znode_close(znode_handle* handle) {
	struct znode_t* znode = (struct znode_t*)handle;
	if(znode->zhandle_)
		zookeeper_close(znode->zhandle_);
	znode_free(znode);
}


void znode_add_watch_path(znode_handle* handle, const char* path, int is_watch_child){
	struct znode_t* znode = (struct znode_t*)handle;
	if (znode_is_watch_path(znode, path))
		return;
	struct znode_watch_path_t* node = (struct znode_watch_path_t*)zm_malloc(sizeof(struct znode_watch_path_t));
	node->is_watch_child = is_watch_child;
	int path_len = strlen(path);
	node->path = (char*)zm_malloc(path_len + 1);
	memcpy(node->path, path, path_len);
	node->path[path_len] = '\0';
	HASH_ADD_STR(znode->watch_node_, path, node);
	if(is_watch_child){
		set_watch_child(znode,path);
	}else{
		set_watch(znode,path);
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


void znode_update(znode_handle* zhandle) {
	struct znode_t* znode = (struct znode_t*)zhandle;
	for(;;){
		struct znode_event_t* ev = (struct znode_event_t*)safe_queue_pop_front(&znode->zevent_);
		if (!ev) {
			break;
		}
		switch(ev->info_type)
		{
			case info_type_event:
			{
				int   type = ev->info.event_info.type_;
				int   state = ev->info.event_info.state_;
				char* path = ev->info.event_info.path_;
				if(type == ZOO_CREATED_EVENT
					|| type == ZOO_DELETED_EVENT
					|| type == ZOO_CHANGED_EVENT){
					if (znode_is_watch_path(znode, path) == 1){
						set_watch(znode, path);
					}
				}
				else if(type == ZOO_CHILD_EVENT)
				{
					if (znode_is_watch_path(znode, path) == 1) {
						set_watch_child(znode, path);
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
					znode->cb_.on_watch_(znode, & (ev->info.event_info) );
				}
			}
			break;

			case info_type_data:
			{
				if (znode->cb_.on_async_data_) {
					znode->cb_.on_async_data_(znode, &(ev->info.data_info) );
				}
			}
			break;
		}
		znode_event_free(ev);
	}
}


//------------sync api----------------------------------

int znode_create(znode_handle* handle, const char* path, const char* value, int value_len, int flags) {
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_create(znode->zhandle_, path, value, value_len,
		&ZOO_OPEN_ACL_UNSAFE, flags, (char*)0,0);
	return rc;
}

int znode_exists(znode_handle* handle, const char* path) {
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_exists(znode->zhandle_, path, 1, NULL);
	return rc;
}

int znode_delete(znode_handle* handle, const char* path, int version) {
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_delete(znode->zhandle_, path, version);
	return rc;
}

int znode_get(znode_handle* handle, const char* path, char* buffer, int* buffer_len, int* version) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct Stat stat;
	stat.version = 0;
	int rc = zoo_get(znode->zhandle_, path, 1, buffer, buffer_len, &stat);
	*version = stat.version;
	return rc;
}

int znode_set(znode_handle* handle, const char* path, const char* buffer, int buffer_len, int version) {
	struct znode_t* znode = (struct znode_t*)handle;
	int rc = zoo_set(znode->zhandle_, path, buffer, buffer_len, version);
	return rc;
}

int znode_get_children(znode_handle* handle, const char* path, int* count, char** child_paths, int* version) {
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


//------------------async api---------------------------------

static void znode_void_completion_cb(int rc, const void *data) {
	if (data == NULL) return;
	struct znode_event_t* d = (struct znode_event_t*)data;
	struct znode_data_info_t* info = &((d->info).data_info);
	struct znode_t* znode = (struct znode_t*)info->znode_;
	info->rc_ = rc;
	//printf("\n znode_void_completion_cb,op_type:%d,path:%s",info->op_type_,info->path_);
	safe_queue_push_back(&znode->zevent_, &(d->node));
}



static void znode_data_completion_cb(int rc, const char *value, int value_len,
	const struct Stat *stat, const void *data) {
	if (data == NULL) return;
	struct znode_event_t* d = (struct znode_event_t*)data;
	struct znode_data_info_t* info = &((d->info).data_info);
	struct znode_t* znode = (struct znode_t*)info->znode_;
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
	if (data == NULL) return;
	struct znode_event_t* d = (struct znode_event_t*)data;
	struct znode_data_info_t* info = &((d->info).data_info);
	struct znode_t* znode = (struct znode_t*)info->znode_;	
	info->rc_ = rc;
	if(stat)
		info->version_ = stat->version;
	//printf("\n znode_stat_completion_cb,op_type:%d,path:%s",info->op_type_,info->path_);
	safe_queue_push_back(&znode->zevent_, &(d->node));
}
static void znode_aset_stat_completion_cb(int rc, const struct Stat *stat, const void *data) {
	if (data == NULL) return;
	struct znode_event_t* d = (struct znode_event_t*)data;
	struct znode_data_info_t* info = &((d->info).data_info);
	struct znode_t* znode = (struct znode_t*)info->znode_;	
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
	if (data == NULL) return;
	struct znode_event_t* d = (struct znode_event_t*)data;
	struct znode_data_info_t* info = &((d->info).data_info);
	struct znode_t* znode = (struct znode_t*)info->znode_;
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
	if (data == NULL) return;
	struct znode_event_t* d = (struct znode_event_t*)data;
	struct znode_data_info_t* info = &((d->info).data_info);
	struct znode_t* znode = (struct znode_t*)info->znode_;
	info->rc_ = rc;
	if (strings && strings->count > 0){
		info->strings_.count = strings->count;
		info->strings_.data = (char**)zm_malloc(sizeof(char*)*strings->count);
		int i;
		for (i = 0; i<strings->count; ++i)
		{
			int value_len = strlen(strings->data[i]);
			info->strings_.data[i] = (char*)zm_malloc(value_len + 1);
			memcpy(info->strings_.data[i], strings->data[i], value_len);
			info->strings_.data[i][value_len] = '\0';
		}
	}
	//printf("\n znode_strings_completion_cb,op_type:%d,path:%s",info->op_type_,info->path_);
	safe_queue_push_back(&znode->zevent_,&(d->node));
}


static void znode_strings_stat_completion_cb(int rc,
	const struct String_vector *strings, const struct Stat* stat, const void *data) {
	if (data == NULL) return;
	struct znode_event_t* d = (struct znode_event_t*)data;
	struct znode_data_info_t* info = &((d->info).data_info);
	struct znode_t* znode = (struct znode_t*)info->znode_;
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
			int value_len = strlen(strings->data[i]);
			info->strings_.data[i] = (char*)zm_malloc(value_len + 1);
			memcpy(info->strings_.data[i], strings->data[i], value_len);
			info->strings_.data[i][value_len] = '\0';
		}
	}
	//printf("\n znode_strings_stat_completion_cb,op_type:%d,path:%s",info->op_type_,info->path_);
	safe_queue_push_back(&znode->zevent_, &(d->node));
}

//---------------------------------------------------------------------------------------------------

int znode_acreate(znode_handle* handle, int session, const char* path, const char* value, int value_len, int flags) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_event_t* data = znode_data_create(session, ZNODE_OP_CREATE, path, znode);
	int rc = zoo_acreate(znode->zhandle_, path, value, value_len,
		&ZOO_OPEN_ACL_UNSAFE, flags, znode_string_completion_cb, data);
	return rc;
}

int znode_adelete(znode_handle* handle, int session, const char* path, int version) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_event_t* data = znode_data_create(session, ZNODE_OP_DELETE, path, znode);
	int ret = zoo_adelete(znode->zhandle_, path, version, znode_void_completion_cb, data);
	return ret;
}

int znode_aexists(znode_handle* handle, int session, const char* path) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_event_t* data = znode_data_create(session, ZNODE_OP_EXISTS, path, znode);
	int ret = zoo_aexists(znode->zhandle_, path, 1, znode_stat_completion_cb, data);
	return ret;
}

int znode_aget(znode_handle* handle, int session, const char* path) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_event_t* data = znode_data_create(session, ZNODE_OP_GET, path, znode);
	int ret = zoo_aget(znode->zhandle_, path, 1, znode_data_completion_cb, data);
	return ret;
}

int znode_aset(znode_handle* handle, int session, const char* path, const char* buffer, int buffer_len, int version) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_event_t* data = znode_data_create(session, ZNODE_OP_SET, path, znode);
	int ret = zoo_aset(znode->zhandle_, path, buffer, buffer_len, version, znode_aset_stat_completion_cb, data);
	return ret;
}

int znode_aget_children(znode_handle* handle, int session, const char* path) {
	struct znode_t* znode = (struct znode_t*)handle;
	struct znode_event_t* data = znode_data_create(session, ZNODE_OP_GET_CHILDREN, path, znode);
	int ret = zoo_aget_children2(znode->zhandle_, path, 1, znode_strings_stat_completion_cb, data);
	return ret;
}
