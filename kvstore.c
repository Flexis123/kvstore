#include <semaphore.h>
#include <assert.h>
#include <stdlib.h>
#include <pthread.h>

#include "server_utils.h"
#include "common.h"
#include "request_dispatcher.h"
#include "hash.h"
#include "kvstore.h"

// DO NOT MODIFY THIS.
// ./check.py assumes the hashtable has 256 buckets.
// You should initialize your hashtable with this capacity.
#define HT_CAPACITY 256
#define WORKER_THREADS 1000

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
    }
    return item;
}

int set_request(int socket, struct request *request)
{
    size_t len = 0;
    size_t expected_len = request->msg_len;

    // 1. Lock the hashtable entry. Create it if the key is not in the store.
    unsigned h = calculate_pos(request);
    hash_item_t* itm = get_item(request->key, h);
    if(itm == NULL)
    {
        itm = malloc(sizeof(hash_item_t));
        itm->next = ht->items[h];
        itm->prev = NULL;

        if(ht->items[h] != NULL)
        {
            ht->items[h]->prev = itm;
        }

        ht->items[h] = itm;

        itm->key = malloc(strlen(request->key) + 1);
        strcpy(itm->key, request->key);
    }

   
        // 2. Read the payload from the socket
        // Note: Clients may send a partial chunk of the payload so you should not wait
        // for the full data to be available before write in the hashtable entry.

    printf("expected len %lu\n", expected_len);
    
    size_t buf_size = 4096;
    size_t curr_buf_size;

    itm->value_size = expected_len;
    itm->value = malloc(expected_len + 1);
    itm->value[expected_len] = '\0';

    while(len < expected_len)
    {
        curr_buf_size = expected_len - len;
        curr_buf_size = curr_buf_size >= buf_size ? buf_size : curr_buf_size;

        char buf[curr_buf_size];

        int rcved = read_payload(socket, request, curr_buf_size, buf);
        if((size_t)rcved != curr_buf_size)
        {
            send_response(socket, STORE_ERROR, 0, NULL);
            request->connection_close = 1;
            return -1;
        }

        memcpy(itm->value + len, buf, curr_buf_size);
        len += curr_buf_size;
    }

    // 4. Unlock the entry in the store to finalize the insertion.
    // This allow other threads to read the entry.

    // It checks if the payload has been fully received .
    // It also reads the last char of the request which should be '\n'
    if(check_payload(socket, request, expected_len) != 0)
    {
        send_response(socket, PARSING_ERROR, 0, NULL);
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
        send_response(socket, KEY_ERROR, 0, NULL);
        return -1;
    }

    send_response(socket, OK, itm->value_size, itm->value);
    return 0;
}

int del_request(int socket, struct request *request)
{
    int h = calculate_pos(request);
    hash_item_t* item = get_item(request->key, h);

    if(item == NULL)
    {
        send_response(socket, KEY_ERROR, 0, NULL);
        return -1;
    }

    if(item->prev != NULL) item->prev->next = item->next;
    if(item->next != NULL) item->next->prev = item->prev;
    if(item == ht->items[h]) ht->items[h] = item->next;

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
    init_table();

    listen_sock = server_init(argc, argv);

    // Initialuze your hashtable.
    // @see kvstore.h for hashtable struct declaration

    pthread_t th[WORKER_THREADS];
    int i = 0;
    for (;;) {
        struct conn_info *conn_info =
            calloc(1, sizeof(struct conn_info));
        if (accept_new_connection(listen_sock, conn_info) < 0) {
            error("Cannot accept new connection");
            free(conn_info);
            continue;
        }
        pthread_create(&th[i], NULL, main_job, conn_info);
        i++;
    } 

    return 0;
}