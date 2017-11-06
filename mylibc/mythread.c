


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
#include "linked.h"
#include "safe_queue.h"

#include <unistd.h>
#include <iostream>
#include <thread>


#include "linked.c"
#include "smemory.c"
#include "memory_trace.c"
#include "safe_queue.c"

struct safe_queue_t queue;

struct test_node_t {
	struct node_t node;
	int x;
};

void thread_task1() {
	while(1){
		struct test_node_t* p = (struct test_node_t*)smem_malloc(sizeof(struct test_node_t),1,__FILE__,__LINE__);
		p->x = 12;
		safe_queue_push_back(&queue,&(p->node));
		usleep(1000*10);
	}
}

void thread_task2() {
	while(1){
		for(;;){
			struct test_node_t* p = (struct test_node_t*)safe_queue_pop_front(&queue);
			if(!p){
				break;
			}
			
			if(p->x != 12)
			{
				printf("\r\n error!!!!!");
			}
			printf("\r\n recv p!!! %d",p->x);
			smem_free(p,1);
		}
		usleep(1000*10);
	}
}

void thread_task3() {
	while(1)
	{
		smem_debug_print();
		usleep(1000*3000);
	}
}

int main() {

	smem_debug_enable(1);
	smem_init();

	safe_queue_init(&queue);

	std::thread t1(thread_task1);
	std::thread t11(thread_task1);
	std::thread t12(thread_task1);

	std::thread t2(thread_task2);
	std::thread t3(thread_task3);

	t1.join();
	t11.join();
	t12.join();

	t2.join();
	t3.join();

	safe_queue_uninit(&queue);
	smem_uninit();
}
