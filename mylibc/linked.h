

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

	struct node_t {
		struct node_t* next;
	};

	void node_init(struct node_t* node);
	void node_push_back(struct node_t** head, struct node_t* new_node);
	void node_push_front(struct node_t** head, struct node_t* new_node);
	struct node_t* node_pop_front(struct node_t** head);
	void node_remove(struct node_t** head, struct node_t* remove_node);


#ifdef __cplusplus
}
#endif