
#include "memory_trace.h"
#include <stdio.h>
#include <assert.h>
#include "memory.h"
#include"uthash.h"


struct trace_struct_t {  
	void* key;					/* key  */
	UT_hash_handle hh;			/* makes this structure hashable */  
	struct mem_trace_info info;
};  

static struct trace_struct_t* g_root;
void on_malloc(void* p,int size,char* file,int line)
{
#ifdef USER_MEM_TRACE
	struct trace_struct_t* t = (struct trace_struct_t*)malloc(sizeof(struct trace_struct_t));
	t->key = p;
	(t->info).p = p;
	(t->info).size = size;
	(t->info).file = file;
	(t->info).line = line;
	HASH_ADD_PTR(g_root, key, t);
#endif
}
	
void on_free(void* p)
{
#ifdef USER_MEM_TRACE
	void* key = p;
	struct trace_struct_t* t = (struct trace_struct_t *)0;
	HASH_FIND_PTR(g_root, &key, t);
	if (t) {
		HASH_DEL(g_root, t);
		free(t);
	}
#endif
}

void memory_trace_print()
{
	struct trace_struct_t *t, *tmp;
	HASH_ITER(hh, g_root, t, tmp) {
		printf("\r\n ptr:0x%x,size:%d,file:%s,line:%d ", (t->info).p, (t->info).size, (t->info).file, (t->info).line);
	}
}

void* my_zoo_malloc(int size, char* file, int line)
{
	void* p = malloc(size);
	on_malloc(p, size, file, line);
	return p;
}

void my_zoo_free(void*p)
{
	on_free(p);
	free(p);
}



