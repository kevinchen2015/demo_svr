


#include "linked.h"
#include <stdio.h>


void node_init(struct node_t* node) {
	if (!node) return;
	node->next = (struct node_t*)0;
}

void node_push_back(struct node_t** head, struct node_t* new_node){
	if (!(*head))
	{
		*head = new_node;
		return;
	}
	struct node_t* node = *head;
	while (node->next)
	{
		node = node->next;
	}
	node->next = new_node;
}

void node_push_front(struct node_t** head, struct node_t* new_node) {
	if (!(*head))
	{
		*head = new_node;
		return;
	}
	new_node->next = *head;
	*head = new_node;
}

struct node_t* node_pop_front(struct node_t** head) {
	if (!(*head)) {
		return (struct node_t*)0;
	}
	struct node_t* node = *head;
	*head = node->next;
	node->next = (struct node_t*)0;
	return node;
}


void node_remove(struct node_t** head, struct node_t* remove_node){
	if (!(*head)) return;
	if ((*head) == remove_node)
	{
		*head = (struct node_t*)0;
		return;
	}
	struct node_t* node = *head;
	struct node_t* last_node = *head;
	do 
	{
		if (node == remove_node)
		{
			last_node->next = node->next;
			return;
		}
		last_node = node;
		node = node->next;
	} while (node);
}
