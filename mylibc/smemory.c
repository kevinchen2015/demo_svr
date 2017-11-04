
#include "smemory.h"
#include "linked.h"
#include "spinlock.h"

#include <stdio.h>
#include <assert.h>
#define PRE_MALLOC_NUM 8

static int gs_debug_enable = 0;

static struct mem_node_t{
	struct node_t node; //must first member!

	size_t size;
	union 
	{
		size_t			n;
		unsigned char	chs[4];
	}param;
};

static enum {
	mem_size1 = 0, 
	mem_size2,	   
	mem_size3,	   
	mem_size4,	   

	mem_size_large,//more
};


static const size_t s_mem_size[] = {
	128,
	512,
	2048,
	10240,
};

static struct mem_node_debug_t {
	int node_num;
	int total_size;
	int used_size;
};


static struct mem_pool{
	struct mem_node_t* free_pool[mem_size_large];
	struct spinlock lock;
	struct mem_node_debug_t debug[mem_size_large];
};

static struct mem_pool s_mem_pool;

void 
smem_init() {
	for (int i = 0; i < mem_size_large; ++i) {
		s_mem_pool.free_pool[i] = (struct mem_node_t*)0;
		s_mem_pool.debug[i].node_num = 0;
		s_mem_pool.debug[i].total_size = 0;
		s_mem_pool.debug[i].used_size = 0;
	}
	SPIN_INIT(&s_mem_pool);
}

void
smem_uninit() {

	SPIN_LOCK(&s_mem_pool);

	smem_debug_enable(1);
	smem_debug_print();
	smem_debug_enable(0);

	for (int i = 0; i < mem_size_large; ++i) {
		struct mem_node_t* node = s_mem_pool.free_pool[i];
		while (node)
		{
			void* p = node;
			node = (struct mem_node_t*)((node->node).next);
			free(p);
		}
		s_mem_pool.free_pool[i] = (struct mem_node_t*)0;
		s_mem_pool.debug[i].node_num = 0;
		s_mem_pool.debug[i].total_size = 0;
		s_mem_pool.debug[i].used_size = 0;
	}
	SPIN_UNLOCK(&s_mem_pool);

	SPIN_DESTROY(&s_mem_pool);
}

void* 
smem_malloc(size_t size,int thread_safe, char* file, int line){
	if (size > s_mem_size[mem_size_large - 1])
	{
		size_t all_size = size + sizeof(struct mem_node_t);
		char* p = (char*)malloc(all_size);
		struct mem_node_t* node = (struct mem_node_t*)p;
		node->size = size;
		node->param.chs[0] = 's';
		node->param.chs[1] = 'm';
		node->param.chs[2] = mem_size_large;
		node->param.chs[3] = 0xff;
		
		if (gs_debug_enable == 1)
		{
			if (thread_safe) {
				SPIN_LOCK(&s_mem_pool);
			}
			on_malloc(p, all_size, file, line);
			if (thread_safe) {
				SPIN_UNLOCK(&s_mem_pool);
			}
		}

		p += (sizeof(struct mem_node_t));
		memset(p, 0x00, size);
		
		return p;
	}
	//others
	for (int i = 0; i < (mem_size_large - 1); ++i) {
		if (size < s_mem_size[i]){
			if (thread_safe) {
				SPIN_LOCK(&s_mem_pool);
			}
			struct mem_node_t* node = (struct mem_node_t*)node_pop_front(&(( ((s_mem_pool.free_pool[i])))));
			if(node)
				s_mem_pool.debug[i].used_size += size;
			if (thread_safe) {
				SPIN_UNLOCK(&s_mem_pool);
			}
			if (node){
				if  (!(node->param.chs[0] == 's' && node->param.chs[1] == 'm')) {
					printf("\r\n smem_malloc ptr is not smem node data !!!");
					return (struct mem_node_t*)0;
				}
				if (node->param.chs[3] == 0xff){
					printf("\r\n smem_malloc remalloc !!!");
					return (struct mem_node_t*)0;
				}
				if (node->param.chs[2] != i) {
					printf("\r\n smem_malloc pool type error !!!!");
					return (struct mem_node_t*)0;
				}
				char* p = (char*)node;
				node->size = size;
				node->param.chs[3] = 0xff;
				p += (sizeof(struct mem_node_t));
				return p;
			}else {
				size_t all_size = s_mem_size[i] + sizeof(struct mem_node_t);
				char* p = (char*)malloc(all_size * PRE_MALLOC_NUM);

				if (thread_safe) {
					SPIN_LOCK(&s_mem_pool);
				}
				s_mem_pool.debug[i].node_num += PRE_MALLOC_NUM;
				s_mem_pool.debug[i].total_size += (s_mem_size[i] * PRE_MALLOC_NUM);
				if(gs_debug_enable == 1)
					on_malloc(p, all_size * PRE_MALLOC_NUM, file, line);
				s_mem_pool.debug[i].used_size += size;
				if (thread_safe) {
					SPIN_UNLOCK(&s_mem_pool);
				}

				for(int j=0;j<PRE_MALLOC_NUM;++j)
				{
					struct mem_node_t* node = (struct mem_node_t*)p;
					node_init(node);
					node->size = 0;
					node->param.chs[0] = 's';
					node->param.chs[1] = 'm';
					node->param.chs[2] = i;
					node->param.chs[3] = 0xff;
					if(j<PRE_MALLOC_NUM-1)
					{
						smem_free(p+sizeof(struct mem_node_t),thread_safe);
						p += all_size;
					}
					else
					{
						node->size = size;
					}
				}
				p += sizeof(struct mem_node_t);
				memset(p, 0x00, s_mem_size[i]);
				return p;
			}
			break;
		}
	}
	return (struct mem_node_t*)0;
}

void 
smem_free(void* p, int thread_safe){
	if (!p) return;
	char* ptr = (void*)p;
	ptr -= (sizeof(struct mem_node_t));
	struct mem_node_t* node = (struct mem_node_t*)ptr;
	if (node->param.chs[0] == 's' && node->param.chs[1] == 'm'){
		if (node->param.chs[3] == 0xff){
			if (node->param.chs[2] >= mem_size_large){
				if (gs_debug_enable == 1)
				{
					if (thread_safe) {
						SPIN_LOCK(&s_mem_pool);
					}
					on_free(p);
					if (thread_safe) {
						SPIN_UNLOCK(&s_mem_pool);
					}
				}
				free(p);  
			}
			else {
				//others
				int idx = node->param.chs[2];
				if (s_mem_size[idx] >= node->size) {
					
				}else {
					assert(0);
					printf("\r\n smem_free pool type error !!!");
				}
				int size = node->size;
				node->param.chs[3] = 0x00;
				node->size = 0;
				memset(p, 0x00, s_mem_size[idx]);
				if (thread_safe) {
					SPIN_LOCK(&s_mem_pool);
				}
				s_mem_pool.debug[idx].used_size -= size;
				node_push_front(&(((s_mem_pool.free_pool[idx]))),(struct node_t*)node);
				if (thread_safe) {
					SPIN_UNLOCK(&s_mem_pool);
				}
			}
		}else {
			printf("\r\n smem_free ptr is already released !!!");
		}
	}else{
		printf("\r\n smem_free ptr is not smem node data !!!");
		assert(0);  //error!!!!

		if (gs_debug_enable == 1)
		{
			if (thread_safe) {
				SPIN_LOCK(&s_mem_pool);
			}
			on_free(p);
			if (thread_safe) {
				SPIN_UNLOCK(&s_mem_pool);
			}
		}
		free(p);
	}
}

void 
smem_debug_enable(int enable) {
	gs_debug_enable = enable;
}

void
smem_debug_print() {
	printf("\r\n ========= mem debug print ============");
	printf("\r\n smem pool info:");
	for (int i = 0; i < mem_size_large;++i) {
		printf("\r\n i:%d,page_size:%d,page_num:%d,total_size:%d,used_size:%d",
			i, s_mem_size[i], s_mem_pool.debug[i].node_num, s_mem_pool.debug[i].total_size, s_mem_pool.debug[i].used_size);
	}
	printf("\r\n smem system info:");
	if (gs_debug_enable == 1) {
		memory_trace_print();
	}
	printf("\r\n ======================================");
}


