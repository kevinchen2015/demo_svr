

#pragma once

#include "memory.h"

#ifdef __cplusplus
extern "C" {
#endif

#define USE_MEM_POOL 

#include "memory_trace.h"

	void smem_init();
	void smem_uninit();
	void* smem_malloc(size_t size, int thread_safe, char* file, int line);
	void smem_free(void* p, int thread_safe);
	void smem_debug_enable(int enable);
	void smem_debug_print();

#ifdef USE_MEM_POOL

#define zm_init() smem_init()
#define zm_uninit() smem_uninit()

#define zm_malloc(size) smem_malloc(size,1,__FILE__,__LINE__)
#define zm_free(p)	smem_free(p,1)

#define zm_unsafe_malloc(size) smem_malloc(size,0,__FILE__,__LINE__)
#define zm_unsafe_free(p)	smem_free(p,0)


#else

#define zm_init() 
#define zm_uninit()

#define zm_malloc(size) my_zoo_malloc(size,__FILE__,__LINE__)
#define zm_mfree(p)	my_zoo_free(p)

#define zm_unsafe_malloc(size) my_zoo_malloc(size,__FILE__,__LINE__)
#define zm_unsafe_free(p)	my_zoo_free(p)

#endif

#ifdef __cplusplus
}
#endif