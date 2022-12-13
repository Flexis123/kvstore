#include <semaphore.h>
#include <assert.h>
#include <stdlib.h>
#include <pthread.h>

#include "server_utils.h"
#include "common.h"
#include "request_dispatcher.h"
#include "hash.h"
#include "kvstore.h"

void init_table()
{
    ht = malloc(sizeof(hashtable_t));
    ht->capacity = HT_CAPACITY;
    ht->items = calloc(HT_CAPACITY, sizeof(hash_item_t*));
    ht->user = malloc(sizeof(struct user_ht));

    for(unsigned i = 0; i < HT_CAPACITY; i++) pthread_rwlock_init(&ht->user->bucket_mutexes[i], NULL);
}

void destroy_table()
{
    for(unsigned i = 0; i < HT_CAPACITY; i++) {
        pthread_rwlock_destroy(&ht->user->bucket_mutexes[i]);

        hash_item_t* curr_item = ht->items[i];

        while(curr_item != NULL)
        {
            pthread_rwlock_destroy(&curr_item->user->item_mutex);
            curr_item = curr_item->next;
        }
    }
    free(ht);
    ht = NULL;
}

unsigned calculate_pos(struct request *request)
{
    return hash(request->key) % ht->capacity;
}

hash_item_t* get_item(char* key, unsigned h)
{
    hash_item_t* item = NULL;
    hash_item_t* curr_item = ht->items[h];

    while(curr_item != NULL)
    {
        if(strcmp(key, curr_item->key) == 0)
        {
            item = curr_item;
            break;
        }
        curr_item = curr_item->next;
    }
    return item;
}

int remove_item(char* key, unsigned h)
{

    hash_item_t* item = get_item(key, h);
    if(item != NULL)
    {
        if(item->prev != NULL) item->prev->next = item->next;
        if(item->next != NULL) item->next->prev = item->prev;
        if(item == ht->items[h]) ht->items[h] = item->next;
        return 0;
    }
    return -1;
}

int try_lock(int socket, struct request *request, pthread_rwlock_t* mutex)
{
    if(pthread_rwlock_trywrlock(mutex) != 0)
    {
        request->connection_close = 0;
        send_response(socket, KEY_ERROR, 0, NULL);
        return -1;
    }
    return 0;
}

int set_count = 0;
int set_request(int socket, struct request *request)
{
    set_count++;
    size_t len = 0;
    size_t expected_len = request->msg_len;

    unsigned h = calculate_pos(request);
    hash_item_t* itm = get_item(request->key, h);
    pthread_rwlock_t* item_mutex;
    pthread_rwlock_t* bucket_mutex;

    if(itm == NULL)
    {
        itm = malloc(sizeof(hash_item_t));

        itm->user = malloc(sizeof(struct user_item));
        pthread_rwlock_init(&itm->user->item_mutex, NULL);
        item_mutex = &itm->user->item_mutex;
        bucket_mutex = &ht->user->bucket_mutexes[h];

        if(try_lock(socket, request, bucket_mutex) != 0) return -1;
        pthread_rwlock_wrlock(item_mutex);
        
        itm->next = ht->items[h];
        itm->prev = NULL;


        if(ht->items[h] != NULL)
        {
            ht->items[h]->prev = itm;
        }
        ht->items[h] = itm;

        pthread_rwlock_unlock(bucket_mutex);

        itm->key = malloc(strlen(request->key) + 1);
        itm->value_size = 0;
        itm->value = NULL;
        strcpy(itm->key, request->key);
    }
    else
    {
        item_mutex = &itm->user->item_mutex;
        bucket_mutex = &ht->user->bucket_mutexes[h];
        if(try_lock(socket, request, item_mutex) != 0) {
            pr_debug("Socket nr %d denied mutex acces\n", socket);
            return -1;
        }
    }

    char* value = malloc(expected_len + 1);
    value[expected_len] = '\0';

    int buf_size = 0;
    const int max_buf_size = 4096;

    

    pr_debug("Reading msg of size %lu for key %s in socket %d\n", expected_len, request->key, socket);
    while(len < expected_len)
    {
        buf_size = expected_len - len;
        buf_size = buf_size >= max_buf_size ? max_buf_size : buf_size;
        char buf[buf_size];

        int rcved = read_payload(socket, request, buf_size, buf);
        pr_debug("Reading payload of size %d for key %s in socket %d\n", buf_size, request->key, socket);
        if(rcved != buf_size)
        {
            if(pthread_rwlock_unlock(item_mutex) == 0) pr_debug("Unlocked item_mutex on socket nr %d\n", socket);

            if(try_lock(socket, request, bucket_mutex) != 0) return -1;
            remove_item(request->key, h);
            pthread_rwlock_unlock(bucket_mutex);
            request->connection_close = 0;
            send_response(socket, STORE_ERROR, 0, NULL);
            return -1;
        }

        memcpy(value + len, buf, buf_size);
        len += buf_size;
    }

    pr_debug("Checking payload  of key %s in socket %d\n", request->key, socket);
    if(check_payload(socket, request, expected_len) != 0)
    {
        pthread_rwlock_unlock(item_mutex);

        if(try_lock(socket, request, bucket_mutex) != 0) return -1;
        remove_item(request->key, h);
        pthread_rwlock_unlock(bucket_mutex);
        request->connection_close = 0;
        send_response(socket, STORE_ERROR, 0, NULL);
        return -1;
    }
    

    pr_debug("Assigining value of key %s in socket %d\n", request->key, socket);

    itm->value = value;
    itm->value_size = expected_len;

    pthread_rwlock_unlock(item_mutex);

    send_response(socket, OK, 0, NULL);
    return len;
}

int get_request(int socket, struct request *request)
{
    int h = calculate_pos(request);
    hash_item_t* itm = get_item(request->key, h);

    if(itm == NULL)
    {
        if(send_response(socket, KEY_ERROR, 0, NULL) == 0) pr_info("GET response sent KEY_ERROR\n");
        return -1;
    }

    if(send_response(socket, OK, itm->value_size, itm->value) == 0) pr_info("GET response sent OK\n");
    return 0;
}

int del_request(int socket, struct request *request)
{
    int h = calculate_pos(request);
    if(remove_item(request->key, h) != 0)
    {
        send_response(socket, KEY_ERROR, 0, NULL);
        return -1;
    }

    send_response(socket, OK, 0, NULL);
    return 0;
}

void *main_job(void *arg)
{
    int method;
    struct conn_info *conn_info = arg;
    struct request *request = allocate_request();
    request->connection_close = 0;

    pr_info("Starting new session from %s:%d sock_nr %d\n",
        inet_ntoa(conn_info->addr.sin_addr),
        ntohs(conn_info->addr.sin_port), conn_info->socket_fd);

    do {
        method = recv_request(conn_info->socket_fd, request);
        // Insert your operations here
        switch (method) {
        case SET:
            set_request(conn_info->socket_fd, request);
            break;
        case GET:
            get_request(conn_info->socket_fd, request);
            break;
        case DEL:
            del_request(conn_info->socket_fd, request);
            break;
        case RST:
            // ./check.py issues a reset request after each test
            // to bring back the hashtable to a known state.
            // Implement your reset command here.
            destroy_table();
            init_table();
            set_count = 0;
            send_response(conn_info->socket_fd, OK, 0, NULL);
            break;
        case STAT:
            break;
        }

        if (request->key) {
            free(request->key);
        }

    } while (!request->connection_close);

    close_connection(conn_info->socket_fd);
    free(request);
    free(conn_info);
    return (void *)NULL;
}


int main(int argc, char *argv[])
{
    int listen_sock;
    listen_sock = server_init(argc, argv);

    // Initialuze your hashtable.
    // @see kvstore.h for hashtable struct declaration
    init_table();


    //init worker threads and job queue

   
    for (;;) {
        struct conn_info *conn_info =
            calloc(1, sizeof(struct conn_info));
        if (accept_new_connection(listen_sock, conn_info) < 0) {
            error("Cannot accept new connection");
            free(conn_info);
            continue;
        }
        pthread_t th;
        pthread_create(&th, NULL, main_job, conn_info);
    } 

    return 0;
}