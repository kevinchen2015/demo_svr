
#include "znode_high.h"
#include "linked.h"
#include "smemory.h"
#include "znode.h"
#include "uthash.h"
#include <stdio.h>
#include <assert.h>

#include <zookeeper.h>
#include <zookeeper_log.h>


struct node_data_t {
	char* path;
	UT_hash_handle hh;
	struct znode_high_data_ info;
}* g_data_root;

struct node_group_t {
	char* 	path;
	int   	child_count;
	char**  child_name;
	UT_hash_handle hh;
}* g_group_root;


static struct znode_high_callback g_cb;
static znode_handle* g_znode_handle;
static int g_session_cnt;
static char temp_path[1024] = {0x00};
static int data_buffer_size = 1024*512;
static char* data_buff = (char*)0;

void znode_set_data_buffer_size(int size){
	if(data_buff)
	{
		zm_free(data_buff);
	}
	data_buffer_size = size;
	data_buff = (char*)zm_malloc(data_buffer_size);
}
static void _node_data_free(struct node_data_t* data) {
	if(data->info.path)
		zm_free(data->info.path);
	if(data->info.value)
		zm_free(data->info.value);
	if(data->path)
		zm_free(data->path);
	zm_free(data);
}

static struct node_data_t* _node_data_create(const char* path,int version,const char* value,int value_len) {
	struct node_data_t* t = (struct node_data_t*)zm_malloc(sizeof(struct node_data_t));
	t->info.version = version;
	int path_len = strlen(path);
	t->path = (char*)zm_malloc(path_len + 1);
	memcpy(t->path, path, path_len);
	t->path[path_len] = '\0';
	t->info.path = (char*)zm_malloc(path_len + 1);
	memcpy(t->info.path, path, path_len);
	t->info.path[path_len] = '\0';
	t->info.value = (char*)zm_malloc(value_len);
	memcpy(t->info.value, value, value_len);
	t->info.value_len = value_len;
	return t;
}

static void _node_data_add(struct node_data_t* data) {
	struct node_data_t* t = (struct node_data_t *)0;
	HASH_FIND_STR(g_data_root, data->path, t);
	if (t) {
		if (data->info.version > t->info.version) {
			//replace!
			HASH_DEL(g_data_root, t);
			_node_data_free(t);
			HASH_ADD_STR(g_data_root, path, data);

			if (g_cb.event_cb) {
				g_cb.event_cb(EVENT_MODIFY, &(data->info));
			}
		}
		else {
			_node_data_free(data);
		}
	}else {
		HASH_ADD_STR(g_data_root, path, data);
		if (g_cb.event_cb) {
			g_cb.event_cb(EVENT_CREATE, &(data->info));
		}
	}
}

static void _node_data_remove(const char* path) {
	znode_remove_watch_path(g_znode_handle,path);
	struct node_data_t* t = (struct node_data_t *)0;
	HASH_FIND_STR(g_data_root,path, t);
	if (t) {
		HASH_DEL(g_data_root, t);
		if (g_cb.event_cb) {
			g_cb.event_cb(EVENT_DELETE, &(t->info));
		}
		_node_data_free(t);
	}
}

struct znode_high_data_* znode_high_get_data(const char* path) {
	struct node_data_t* t = (struct node_data_t *)0;
	HASH_FIND_STR(g_data_root, path, t);
	if (t) {
		return &(t->info);
	}
	return (struct znode_high_data_*)0;
}

static void on_watch(znode_handle* handle, struct znode_event_info_t* info) {
	if (info->type_ == ZOO_CREATED_EVENT 
		|| info->type_ == ZOO_CHANGED_EVENT) {
		znode_aget(g_znode_handle, ++g_session_cnt, info->path_);
	}
	else if (info->type_ == ZOO_DELETED_EVENT) {
		if(znode_is_watch_path_by_substr(g_znode_handle,info->path_)){
			//do nothing 如果是子节点，需要用子节点列表判断
		}
		else{
			_node_data_remove(info->path_);  
		}
	}
	else if (info->type_ == ZOO_CHILD_EVENT)
	{
		znode_aget_children(g_znode_handle, ++g_session_cnt, info->path_);
	}
	else if (info->type_ == ZOO_SESSION_EVENT) {
		if (info->state_ == ZOO_EXPIRED_SESSION_STATE) {
			struct node_data_t *t, *tmp;
			HASH_ITER(hh, g_data_root, t, tmp) {
				znode_aget(g_znode_handle, ++g_session_cnt, t->path);
			}
		}
	}
}

void znode_high_foreach_data(foreach_cb cb) {
	struct node_data_t *t, *tmp;
	HASH_ITER(hh, g_data_root, t, tmp) {
		cb(t->path,&(t->info));
	}
}

static char* _make_full_path(char* parent,char* child_name)
{
	if(!parent || !child_name) return (char*)0;
	if(strlen(child_name)+strlen(parent) < 511){
		int idx = strlen(parent);
		memcpy(temp_path,parent,idx);
		temp_path[idx++] = '/';
		memcpy(temp_path+idx,child_name,strlen(child_name));
		idx += strlen(child_name);
		temp_path[idx] = '\0';
		return temp_path;
	}
	else{
		printf("\r\n path is to long!!!!!!");
	}
	return (char*)0;
}


static void _add_to_node_group(char* parent,int  child_count,char** child_name){
	struct node_group_t* t = (struct node_group_t *)0;
	HASH_FIND_STR(g_group_root,parent, t);

	if(t){
		//比对差异,减少的
		int i;
		for(i = 0;i < t->child_count;++i){
			int finded = 0;
			int j;
			for(j = 0;j < child_count;++j){
				if(memcmp(t->child_name[i],child_name[j],strlen(t->child_name[i])) == 0){
					finded = 1;
				}
			}
			if(finded==0){
				char* full_path = _make_full_path(parent,t->child_name[i]);
				if(full_path){
					//removed
					_node_data_remove(full_path);  
				}
			}
		}
		//刷新最新,简单实现
		for(i = 0;i < t->child_count;++i){
			if(t->child_name[i])
				zm_free(t->child_name[i]);
		}
		if(t->child_count > 0)
		{
			zm_free(t->child_name);
		}
		int num = 0;
		t->child_count = child_count;
		t->child_name = (char**)0;
		if(child_count > 0)
		{
			t->child_name = (char**)zm_malloc(sizeof(char*)*child_count);
			for(i = 0;i < child_count; ++i) {
			num = strlen(child_name[i]);
			t->child_name[i] = (char*)zm_malloc(num+1);
			memcpy(t->child_name[i],child_name[i],num);
			t->child_name[i][num] = '\0';
			}
		}
	}
	else{
		
		t = (struct node_group_t *)zm_malloc(sizeof(struct node_group_t));
		int num = strlen(parent);
		t->path = (char*)zm_malloc(num+1);
		memcpy(t->path,parent,num);
		t->path[num] = '\0';
		t->child_count = child_count;
		t->child_name = (char**)0;
		if(child_count > 0)
		{
			t->child_name = (char**)zm_malloc(sizeof(char*)*child_count);
			int i;
			for(i = 0;i < child_count; ++i) {
			num = strlen(child_name[i]);
			t->child_name[i] = (char*)zm_malloc(num+1);
			memcpy(t->child_name[i],child_name[i],num);
			t->child_name[i][num] = '\0';
			}
		}
		HASH_ADD_STR(g_group_root,path,t);
	}
    int i;
	for (i = 0; i < child_count; ++i) {
		char* _path = _make_full_path(parent,child_name[i]);
		if(_path)
		{
			if(!znode_is_watch_path(g_znode_handle,_path)) {
				znode_add_watch_path(g_znode_handle,_path,0);
				znode_aget(g_znode_handle, ++g_session_cnt, _path);
			}	
		}
	}
}
static void on_async_data(znode_handle* handle, struct znode_data_info_t* info) {
	if (info->op_type_ == ZNODE_OP_GET) {
		if (info->rc_ == ZOK) {
			struct node_data_t* t = _node_data_create(info->path_, info->version_,
				info->data_.value, info->data_.value_len);
			_node_data_add(t);
		}
		else if (info->rc_ == ZNONODE) {
			_node_data_remove(info->path_);
		}
		else {
			if (g_cb.error_cb) {
				g_cb.error_cb(info->path_, info->op_type_, info->rc_);
			}
		}
	}
	else if (info->op_type_ == ZNODE_OP_GET_CHILDREN) {
		if (info->rc_ == ZOK) {
			_add_to_node_group(info->path_,info->strings_.count,info->strings_.data);
		}
		else {
			if (g_cb.error_cb) {
				g_cb.error_cb(info->path_, info->op_type_, info->rc_);
			}
		}
	}
	else if (info->op_type_ == ZNODE_OP_SET) {
		if (info->rc_ == ZOK) {
			struct node_data_t* t = _node_data_create(info->path_, info->version_,
				info->data_.value, info->data_.value_len);
			_node_data_add(t);
		}
		else {
			if (g_cb.error_cb) {
				g_cb.error_cb(info->path_, info->op_type_, info->rc_);
			}
		}
	}
	else if (info->op_type_ == ZNODE_OP_EXISTS
		|| info->op_type_ == ZNODE_OP_CREATE) {
		if (info->rc_ == ZOK) {
			znode_aget(g_znode_handle, ++g_session_cnt, info->path_);
		}
		else {
			if (g_cb.error_cb) {
				g_cb.error_cb(info->path_,info->op_type_,info->rc_);
			}
		}
	}
	else if (info->op_type_ == ZNODE_OP_DELETE) {
		if(znode_is_watch_path_by_substr(g_znode_handle,info->path_)){
			//do nothing 如果是子节点，需要用子节点列表判断
		}
		else{
			_node_data_remove(info->path_);  
		}
	}
}

void znode_high_set_debug_level(int level) {
	znode_set_debug_level(level);
}

int znode_high_init(const char* host, int timeout, struct znode_high_callback* cb) {
	g_data_root = (struct node_data_t*)0;
	g_session_cnt = 0;
	g_cb = *cb;
	znode_set_data_buffer_size(1024*512);
	zinit();
	struct znode_callback_t callback;
	callback.on_watch_ = on_watch;
	callback.on_async_data_ = on_async_data;
	g_znode_handle = znode_open(host,timeout,&callback);
	return 0;
}

void znode_high_uninit() {
	if (g_data_root)
	{
		struct node_data_t *t, *tmp;
		HASH_ITER(hh, g_data_root, t, tmp) {
			_node_data_free(t);
		}
		g_data_root = (struct node_data_t *)0;
	}
	znode_close(g_znode_handle);
	zuninit();
}

void znode_high_update() {
	if (g_znode_handle) {
		znode_update(g_znode_handle);
	}
}

void znode_high_watch_path(const char* path, int is_watch_child) {
	znode_add_watch_path(g_znode_handle, path, is_watch_child);
	if(is_watch_child){
		znode_aget_children(g_znode_handle,++g_session_cnt,path);
	}
}

void znode_high_remove_watch_path(const char* path) {
	znode_remove_watch_path(g_znode_handle, path);
}

//---------------
int znode_high_create(const char* path, const char* value, int value_len, int flags) {
	int ret = znode_create(g_znode_handle, path, value, value_len, flags);
	if (ret == ZOK) {
		struct node_data_t* t = _node_data_create(path, 0,
			value, value_len);
		_node_data_add(t);
	}
	return ret;
}

int	znode_high_exists(const char* path) {
	int ret = znode_exists(g_znode_handle, path);
	if (ret == ZOK) {
		znode_aget(g_znode_handle, ++g_session_cnt, path);
	}
	return ret;
}

int znode_high_delete(const char* path, int version){
	int ret = znode_delete(g_znode_handle, path, version);
	if (ret == ZOK) {
		_node_data_remove(path);
	}
	return ret;
}

int znode_high_get(const char* path, char* buffer, int* buffer_len, int* version) {
	int ret = znode_get(g_znode_handle, path, buffer, buffer_len, version);
	if (ret == ZOK) {
		struct node_data_t* t = _node_data_create(path, *version,
			buffer, *buffer_len);
		_node_data_add(t);
	}
	return ret;
}

int	znode_high_set(const char* path, const char* buffer, int buffer_len, int version) {
	int ret = znode_set(g_znode_handle, path, buffer, buffer_len, version);
	if (ret == ZOK) {
		int   buffer_len = data_buffer_size;
		int   ver;
		ret = znode_high_get(path,data_buff,&buffer_len,&ver);
	}
	return ret;
}

int znode_high_get_children(const char* path, int* count, char** child_paths, int* version){
	int ret = znode_get_children(g_znode_handle, path, count, child_paths, version);
	return ret;
}

//------------------------------------------

int znode_high_acreate(const char* path, const char* value, int value_len, int flags) {
	int ret = znode_acreate(g_znode_handle, ++g_session_cnt,path,value,value_len,flags);
	return ret;
}

int	znode_high_aexists(const char* path) {
	int ret = znode_aexists(g_znode_handle, ++g_session_cnt, path);
	return ret;
}

int znode_high_adelete(const char* path, int version) {
	int ret = znode_adelete(g_znode_handle, ++g_session_cnt, path, version);
	return ret;
}

int znode_high_aget(const char* path) {
	int ret = znode_aget(g_znode_handle, ++g_session_cnt, path);
	return ret;
}

int	znode_high_aset(const char* path, const char* buffer, int buffer_len, int version) {
	int ret = znode_aset(g_znode_handle, ++g_session_cnt, path, buffer, buffer_len, version);
	return ret;
}

int znode_high_aget_children(const char* path) {
	int ret = znode_aget_children(g_znode_handle, ++g_session_cnt, path);
	return ret;
}