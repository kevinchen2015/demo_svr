

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#define znode_handle void

	enum ZNODE_OP
	{
		ZNODE_OP_NONE = 0,
		ZNODE_OP_CREATE = 1,
		ZNODE_OP_DELETE = 2,
		ZNODE_OP_SET = 3,
		ZNODE_OP_GET = 4,
		ZNODE_OP_EXISTS = 5,
		ZNODE_OP_GET_CHILDREN = 6,
		ZNODE_OP_SET_WATCH = 7,
		ZNODE_OP_SET_WATCH_CHILDREN = 8,
	};

	struct znode_event_info_t {
		int type_;
		int state_;
		char* path_;
	};

	struct znode_data_info_t {
		unsigned short session_;
		int op_type_;
		char* path_;
		void* znode_;

		int rc_;
		int version_;
		struct {
			int   	value_len;
			char* 	value;
		} data_;
		struct {
			int 	count;
			char** 	data;
		}strings_;

		char use_session_;
	};

	typedef void(*on_watch_event_cb)(znode_handle* handle, struct znode_event_info_t* info);
	typedef void(*on_async_data_event_cb)(znode_handle* handle, struct znode_data_info_t* info);

	struct znode_callback_t {
		on_watch_event_cb		on_watch_;
		on_async_data_event_cb  on_async_data_;
	};

	//zsystem
	void zinit();
	void zuninit();
	void znode_update();
	void znode_set_debug_level(int level);
	int  znode_is_watch_path_by_substr(znode_handle* handle, const char* path);

	//znode
	znode_handle* znode_open(const char* host, int timeout, struct znode_callback_t* cb);
	void znode_close(znode_handle* handle);
	void znode_add_watch_path(znode_handle* handle, const char* path, int is_watch_child);
	void znode_remove_watch_path(znode_handle* handle, const char* path);
	int  znode_is_watch_path(znode_handle* handle, const char* path);

	//sync api
	int znode_create(znode_handle* handle, const char* path, const char* value, int value_len, int flags);
	int	znode_exists(znode_handle* handle, const char* path);
	int znode_delete(znode_handle* handle, const char* path, int version);
	int znode_get(znode_handle* handle, const char* path, char* buffer, int* buffer_len, int* version);
	int	znode_set(znode_handle* handle, const char* path, const char* buffer, int buffer_len, int version);
	int znode_get_children(znode_handle* handle, const char* path, int* count, char** child_paths, int* version);

	//async api
	int znode_acreate(znode_handle* handle, unsigned short session, const char* path, const char* value, int value_len, int flags);
	int	znode_aexists(znode_handle* handle, unsigned short session, const char* path);
	int znode_adelete(znode_handle* handle, unsigned short session, const char* path, int version);
	int znode_aget(znode_handle* handle, unsigned short session, const char* path);
	int	znode_aset(znode_handle* handle, unsigned short session, const char* path, const char* buffer, int buffer_len, int version);
	int znode_aget_children(znode_handle* handle, unsigned short session, const char* path);

#ifdef __cplusplus
}
#endif