#pragma once


#ifdef WINDOWS

#include <Windows.h>
#include <process.h>

struct mutex_handle_t {
	CRITICAL_SECTION mutex_;
};


void 
mutex_init(struct mutex_handle_t* handle) {
	InitializeCriticalSection(&handle->mutex_);
}

void 
mutex_uninit(struct mutex_handle_t* handle) {
	DeleteCriticalSection(&handle->mutex_);
}
void 
mutex_lock(struct mutex_handle_t* handle) {
	EnterCriticalSection(&handle->mutex_);
}

void 
mutex_unlock(struct mutex_handle_t* handle) {
	LeaveCriticalSection(&handle->mutex_);
}


#else   
// POSIX

#include <pthread.h>

struct mutex_handle_t {
	pthread_mutex_t mutex_;
};


void
mutex_init(struct mutex_handle_t* handle) {
	pthread_mutex_init(&handle->mutex_, (void*)0);
}

void
mutex_uninit(struct mutex_handle_t* handle) {
	pthread_mutex_destroy(&handle->mutex_);
}
void
mutex_lock(struct mutex_handle_t* handle) {
	pthread_mutex_lock(&handle->mutex_);
}

void
mutex_unlock(struct mutex_handle_t* handle) {
	pthread_mutex_unlock(&handle->mutex_);
}


#endif

