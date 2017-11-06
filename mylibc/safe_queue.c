

#include "safe_queue.h"
#include "linked.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void
safe_queue_init(struct safe_queue_t* queue) {
	SPIN_INIT(queue);
	queue->head_ = (struct node_t*)0;
	queue->tail_ = (struct node_t*)0;
	memset(&queue->free_,0x00,sizeof(queue->free_));
}

void
safe_queue_uninit(struct safe_queue_t* queue) {
	if (queue->free_) {
		SPIN_LOCK(queue);
		struct node_t* node = queue->head_;
		while (node) {
			struct node_t* p = node->next;
			queue->free_(node);
			node = p;
		}
		SPIN_UNLOCK(queue);
	}
	SPIN_DESTROY(queue);
}

void 
safe_queue_push_back(struct safe_queue_t* queue,struct node_t* node) {
	SPIN_LOCK(queue);
	if (queue->tail_){
		queue->tail_->next = node;
		queue->tail_ = node;
	}else {
		queue->head_ = node;
		queue->tail_ = node;
	}
	SPIN_UNLOCK(queue);
}

struct node_t*
safe_queue_pop_front(struct safe_queue_t* queue) {
	struct node_t* node = (struct node_t*)0;
	SPIN_LOCK(queue);
	node = node_pop_front(&queue->head_);
	if (!queue->head_) {
		queue->tail_ = (struct node_t*)0;
	}
	SPIN_UNLOCK(queue);
	return node;
}



