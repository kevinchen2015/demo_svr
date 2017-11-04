#pragma once

#include "spinlock.h"

#ifdef __cplusplus
extern "C" {
#endif

	typedef void(*free_fn)(void*p);

	struct node_t;
	struct safe_queue_t {
		struct node_t* head_;
		struct node_t* tail_;
		struct spinlock lock;
		free_fn free_;
	};

	void safe_queue_init(struct safe_queue_t* queue);
	void safe_queue_uninit(struct safe_queue_t* queue);
	void safe_queue_push_back(struct safe_queue_t* queue, struct node_t* node);
	struct node_t* safe_queue_pop_front(struct safe_queue_t* queue);

#ifdef __cplusplus
}
#endif