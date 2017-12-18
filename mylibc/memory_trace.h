

#pragma once

#define USER_MEM_TRACE

#ifdef __cplusplus
extern "C" {
#endif

	struct mem_trace_info {
		void* p;
		int   size;
		char* file;
		int   line;
	};

	/*
	useage:
		on_malloc(p,size,__FILE__,__LINE__);

	*/

	void  on_malloc(void* p, int size, char* file, int line);
	void  on_free(void* p);
	void  memory_trace_print();
	void* my_zoo_malloc(int size, char* file, int line);
	void  my_zoo_free(void*p);

#ifdef __cplusplus
}
#endif
