


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

#include "znode_high.h"
#include "smemory.h"

#include <unistd.h>




const char* host = "192.168.10.53:2181,192.168.10.53:2182,192.168.10.53:2183";
const char* value = "ip:192.168.10.53:9999";

static void
on_data_event(enum znode_high_event event, struct znode_high_data_* data) {
	printf("\r\n ========event:%d,path:%s",(int)event,data->path);
}

static void
on_async_error(char* path, int op_type, int ret) {
	printf("\r\n ========path:%s,ret:%d",path,ret);
}

int main() {

	smem_debug_enable(1);

	znode_high_set_debug_level(4);

	struct znode_high_callback cb;
	cb.event_cb = on_data_event;
	cb.error_cb = on_async_error;
	znode_high_init(host, 10000,&cb);

	znode_high_watch_path("/x1",0);
	znode_high_watch_path("/dp_room", 0);

	int ret = znode_high_acreate("/mytest", value,strlen(value)+1, 1);

	while (1)
	{
		znode_high_update();
		smem_debug_print();
		usleep(1000*1000);
	}

	znode_high_uninit();
}
