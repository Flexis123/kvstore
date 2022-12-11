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
}

void destroy_table()
{
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

int set_count = 0;
int set_request(int socket, struct request *request)
{
    set_count++;
    size_t len = 0;
    size_t expected_len = request->msg_len;

    char* value = malloc(expected_len + 1);
    value[expected_len] = '\0';

    int buf_size = 0;
    const int max_buf_size = 4096;

    pr_debug("Fetching contents of key %s\n", request->key);
    while(len < expected_len)
    {
        buf_size = expected_len - len;
        buf_size = buf_size >= max_buf_size ? max_buf_size : buf_size;
        char buf[max_buf_size];


        int rcved = read_payload(socket, request, buf_size, buf);
        if(rcved != buf_size)
        {
            send_response(socket, STORE_ERROR, 0, NULL);
            request->connection_close = 1;
            return -1;
        }

        memcpy(value + len, buf, buf_size);
        len += buf_size;
    }

    if(check_payload(socket, request, expected_len) != 0)
    {
        send_response(socket, STORE_ERROR, 0, NULL);
        request->connection_close = 1;
        return -1;
    }
    
    unsigned h = calculate_pos(request);
    hash_item_t* itm = get_item(request->key, h);
    if(itm == NULL)
    {

        itm = malloc(sizeof(hash_item_t));
        itm->next = ht->items[h];
        itm->prev = NULL;

        itm->user = malloc(sizeof(struct user_item));
        pthread_mutex_init(&itm->user->mutex, NULL);

        if(ht->items[h] != NULL)
        {
            ht->items[h]->prev = itm;
        }
        ht->items[h] = itm;
        itm->key = malloc(strlen(request->key) + 1);
        strcpy(itm->key, request->key);
    }

    pr_debug("Now in set request nr %d with key %s\n", set_count, request->key);
    if(pthread_mutex_trylock(&itm->user->mutex) != 0)
    {
        send_response(socket, KEY_ERROR, 0, NULL);
        request->connection_close = 1;
        return -1;
    }
    
    itm->value_size = expected_len;
    itm->value = malloc(expected_len + 1);
    memcpy(itm->value, value, expected_len + 1);

    if(pthread_mutex_unlock(&itm->user->mutex) != 0)
    {
        send_response(socket, KEY_ERROR, 0, NULL);
        request->connection_close = 1;
        return -1;
    }

    send_response(socket, OK, 0, NULL);

    // Optionally you can close the connection
    // You should do it ONLY on errors:
    // request->connection_close = 1;
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

    pr_info("Starting new session from %s:%d\n",
        inet_ntoa(conn_info->addr.sin_addr),
        ntohs(conn_info->addr.sin_port));

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