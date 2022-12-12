#ifndef KVSTORE_H
#define KVSTORE_H

#include "common.h"
#include "hash.h"
#include <semaphore.h>

#define HT_CAPACITY 256

struct user_item {
    pthread_rwlock_t item_mutex;
};

struct user_ht {
    pthread_rwlock_t bucket_mutexes[HT_CAPACITY];
};

#define THREAD_POOL_SIZE 200u

struct request_info
{
    int socket;
    struct request* req;
};

struct job_queue
{
    sem_t mutex;
    sem_t empty;
    sem_t full;
    size_t q_size;
    struct request_info** requests;
};

extern struct job_queue *job_q;

#define MUTEX_INIT 1
#define EMPTY_INIT 0 
#define Q_SIZE THREAD_POOL_SIZE * 5

#endif
