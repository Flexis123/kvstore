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

void remove_item(hash_item_t* item, unsigned h)
{
    if(item != NULL)
    {
        if(item->prev != NULL) item->prev->next = item->next;
        if(item->next != NULL) item->next->prev = item->prev;
        if(item == ht->items[h]) ht->items[h] = item->next;
    }
}

int set_request(int socket, struct request *request)
{
    size_t len = 0;
    size_t expected_len = request->msg_len;

    unsigned h = calculate_pos(request);
    pthread_rwlock_t* bucket_mutex = &ht->user->bucket_mutexes[h];

    pthread_rwlock_wrlock(bucket_mutex);

    hash_item_t* itm = get_item(request->key, h);
    pthread_rwlock_t* item_mutex;
    
    int key_err = 0;
    if(itm == NULL)
    {
        itm = malloc(sizeof(hash_item_t));

        itm->user = malloc(sizeof(struct user_item));
        pthread_rwlock_init(&itm->user->item_mutex, NULL);
        item_mutex = &itm->user->item_mutex;
        pthread_rwlock_wrlock(item_mutex);
        
        itm->next = ht->items[h];
        itm->prev = NULL;


        if(ht->items[h] != NULL)
        {
            ht->items[h]->prev = itm;
        }
        ht->items[h] = itm;


        itm->key = malloc(strlen(request->key) + 1);
        itm->value_size = 0;
        itm->value = NULL;
        strcpy(itm->key, request->key);
    }
    else
    {
        item_mutex = &itm->user->item_mutex;
        if(pthread_rwlock_trywrlock(item_mutex) != 0) key_err = 1;
    }

    pthread_rwlock_unlock(bucket_mutex);

    char* value = malloc(expected_len + 1);
    value[expected_len] = '\0';

    int buf_size = 0;
    const int max_buf_size = 4096;

    while(len < expected_len)
    {
        buf_size = expected_len - len;
        buf_size = buf_size >= max_buf_size ? max_buf_size : buf_size;
        char buf[buf_size];

        int rcved = read_payload(socket, request, buf_size, buf);
        if(rcved != buf_size)
        {
            if(!key_err)
            {
                pthread_rwlock_wrlock(bucket_mutex);
                remove_item(itm, h);
                pthread_rwlock_unlock(bucket_mutex);

                pthread_rwlock_unlock(item_mutex);
            }

            request->connection_close = 1;
            send_response(socket, STORE_ERROR, 0, NULL);
            return -1;
        }

        memcpy(value + len, buf, buf_size);
        len += buf_size;
    }

    if(check_payload(socket, request, expected_len) != 0)
    {
        if(!key_err)
        {
            pthread_rwlock_wrlock(bucket_mutex);
            remove_item(itm, h);
            pthread_rwlock_unlock(bucket_mutex);

            pthread_rwlock_unlock(item_mutex);
        }

        request->connection_close = 1;
        send_response(socket, STORE_ERROR, 0, NULL);
        return -1;
    }

    if(key_err)
    {
        send_response(socket, KEY_ERROR, 0, NULL);
        return -1;
    }
    
    itm->value = value;
    itm->value_size = expected_len;

    pthread_rwlock_unlock(item_mutex);

    send_response(socket, OK, 0, NULL);
    return len;
}

int get_request(int socket, struct request *request)
{

    int h = calculate_pos(request);
    pthread_rwlock_t* bucket_mutex = &ht->user->bucket_mutexes[h];

    pthread_rwlock_rdlock(bucket_mutex);
    hash_item_t* item = get_item(request->key, h);

    if(item != NULL)
    {
        pthread_rwlock_t* item_mutex = &item->user->item_mutex;

        if(pthread_rwlock_tryrdlock(item_mutex) != 0) send_response(socket, KEY_ERROR, 0, NULL);
        else send_response(socket, OK, item->value_size, item->value);

        pthread_rwlock_unlock(item_mutex);

    }else send_response(socket, KEY_ERROR, 0, NULL);

    pthread_rwlock_unlock(bucket_mutex);

    return 0;
}

int del_request(int socket, struct request *request)
{
    int key_error = 0;
    int h = calculate_pos(request);

    pthread_rwlock_t* bucket_mutex = &ht->user->bucket_mutexes[h];
    pthread_rwlock_wrlock(bucket_mutex);

    hash_item_t* item = get_item(request->key, h);
    if(item != NULL)
    {
        pthread_rwlock_t* item_mutex = &item->user->item_mutex;
        if(pthread_rwlock_trywrlock(item_mutex) != 0) key_error = 1;
        else
        {
            remove_item(item, h);
            pthread_rwlock_unlock(item_mutex);
        }
    }

    pthread_rwlock_unlock(bucket_mutex);

    if(item == NULL || key_error) send_response(socket, KEY_ERROR, 0, NULL);
    else send_response(socket, OK, 0, NULL);

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
        pr_debug("Received %d request from socket %d\n", method, conn_info->socket_fd);
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