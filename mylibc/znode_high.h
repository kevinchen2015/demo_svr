

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

	
	typedef void(*on_event_cb)(enum znode_high_event event, struct znode_high_data_* data);
	typedef void(*on_error_cb)(char* path, int op_type, int ret);

	struct znode_high_callback
	{
		on_event_cb event_cb;
		on_error_cb error_cb;
	};

	void znode_high_set_debug_level(int level);
	int  znode_high_init(char* host, int timeout, struct znode_high_callback* cb);
	void znode_high_uninit();
	void znode_high_update();

	void znode_high_watch_path(char* path, int is_watch_child);
	void znode_high_remove_watch_path(char* path);

	struct znode_high_data_* znode_high_get_data(char* path);

	//sync api
	int znode_high_create(char* path, char* value, int value_len, int flags);
	int	znode_high_exists(char* path);
	int znode_high_delete(char* path, int version);
	int znode_high_get(char* path, char* buffer, int* buffer_len, int* version);
	int	znode_high_set(char* path, char* buffer, int buffer_len, int version);
	int znode_high_get_children(char* path, int* count, char** child_paths, int* version);

	//async api
	int znode_high_acreate(char* path, char* value, int value_len, int flags);
	int	znode_high_aexists(char* path);
	int znode_high_adelete(char* path, int version);
	int znode_high_aget(char* path);
	int	znode_high_aset(char* path, char* buffer, int buffer_len, int version);
	int znode_high_aget_children(char* path);

#ifdef __cplusplus
}
#endif