
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

	enum znode_high_event {
		EVENT_CREATE,
		EVENT_DELETE,
		EVENT_MODIFY,
	};

	//node base info
	struct znode_high_data_ {
		char* path;
		int   version;
		char* value;
		int   value_len;
	};

	typedef void(*foreach_cb)(char* path,struct znode_high_data_*);
	typedef void(*on_event_cb)(enum znode_high_event event, struct znode_high_data_* data);
	typedef void(*on_error_cb)(char* path, int op_type, int ret);

	struct znode_high_callback
	{
		on_event_cb event_cb;
		on_error_cb error_cb;
	};

	void znode_high_set_debug_level(int level);
	int  znode_high_init(const char* host, int timeout, struct znode_high_callback* cb);
	void znode_high_uninit();
	void znode_high_update();

	void znode_set_data_buffer_size(int size);
	void znode_high_watch_path(const char* path, int is_watch_child);
	void znode_high_remove_watch_path(const char* path);

	struct znode_high_data_* znode_high_get_data(const char* path);
	void znode_high_foreach_data(foreach_cb cb);

	//sync api
	int znode_high_create(const char* path, const char* value, int value_len, int flags);
	int	znode_high_exists(const char* path);
	int znode_high_delete(const char* path, int version);
	int znode_high_get(const char* path, char* buffer, int* buffer_len, int* version);
	int	znode_high_set(const char* path, const char* buffer, int buffer_len, int version);
	int znode_high_get_children(const char* path, int* count, char** child_paths, int* version);

	//async api
	int znode_high_acreate(const char* path, const char* value, int value_len, int flags);
	int	znode_high_aexists(const char* path);
	int znode_high_adelete(const char* path, int version);
	int znode_high_aget(const char* path);
	int	znode_high_aset(const char* path, const char* buffer, int buffer_len, int version);
	int znode_high_aget_children(const char* path);

#ifdef __cplusplus
}
#endif