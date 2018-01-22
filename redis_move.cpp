#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <hiredis.h>
#include <vector>
#include <time.h>
#include <pthread.h>
#include "redis_move.hpp"
#include <unistd.h>

using namespace std;

class AutoLock
{
public:
    pthread_mutex_t* lc;
    AutoLock(pthread_mutex_t* lock) {
        lc = lock;
        pthread_mutex_lock(lc);
    }
    ~AutoLock() {
        pthread_mutex_unlock(lc);
    }
};

RedisClient::RedisClient(string ip, int port, string _passwd, int _timeout)
{
    timeout = _timeout;
    server_port = port;
    setver_ip = ip;
    begin_time = 0;
    _start_thread = false;
    tid = NULL;
    tid_num = 0;
    passwd = _passwd;
    
    pthread_mutex_init(&queue_lock, NULL);
    pthread_mutex_init(&cmd_lock, NULL);
    pthread_cond_init(&cond, NULL);
}

RedisClient::~RedisClient()
{
    pthread_mutex_lock(&queue_lock);
    while(!clients.empty())
    {
        redisContext *ctx = clients.front();
        redisFree(ctx);
        clients.pop();
    }
    pthread_mutex_unlock(&queue_lock);
    pthread_mutex_destroy(&queue_lock);
    
    pthread_mutex_lock(&cmd_lock);
    pthread_mutex_unlock(&cmd_lock);
    pthread_mutex_destroy(&cmd_lock);
    pthread_cond_destroy(&cond);
}

int RedisClient::exec_cmd(const string cmd, string* response, vector<std::string>* keys, int* intera)
{
    redisReply *reply = exec_cmd(cmd);
    if (!reply) {
        printf("error: reply is null\n");
        return REDIS_REPLY_ERROR;
    }
    parse_reply(reply, true, response, keys, intera);
    int type = reply->type;
    freeReplyObject(reply);

    return type;
}

redisReply* RedisClient::exec_cmd(const string cmd)
{
    redisContext *ctx = create_context();
    if(ctx == NULL) return NULL;

    redisReply *reply = (redisReply*)redisCommand(ctx, cmd.c_str());

    release_context(ctx, reply != NULL);

    return reply;
}

redisContext* RedisClient::create_context()
{
    {
        pthread_mutex_lock(&queue_lock);
        if(!clients.empty()) {
            redisContext *ctx = clients.front();
            clients.pop();
            pthread_mutex_unlock(&queue_lock);
            return ctx;
        }
        pthread_mutex_unlock(&queue_lock);
    }

    time_t now = time(NULL);
    if(now < begin_time + maxinterval) return NULL;

    struct timeval tv;
    tv.tv_sec = timeout / 1000;
    tv.tv_usec = (timeout % 1000) * 1000;;
    redisContext *ctx = redisConnectWithTimeout(setver_ip.c_str(), server_port, tv);
    if(ctx == NULL || ctx->err != 0)
    {
        if(ctx != NULL) redisFree(ctx);

        begin_time = time(NULL);
        
        return NULL;
    }

    if (passwd != "" && passwd != "-") {
        string cmd = "AUTH " + passwd;
        redisReply *reply = (redisReply*)redisCommand(ctx, cmd.c_str());
        string rep;
        parse_reply(reply, true, &rep, NULL, NULL);
        if (reply->type == REDIS_REPLY_ERROR) {
            printf("error: passwd failed, repons=%s\n", rep.c_str());
            exit(0);
        }
        release_context(ctx, reply != NULL);
    }

    return ctx;
}

void RedisClient::release_context(redisContext *ctx, bool active)
{
    if(ctx == NULL) return;
    if(!active) {redisFree(ctx); return;}

    pthread_mutex_lock(&queue_lock);
    clients.push(ctx);
    pthread_mutex_unlock(&queue_lock);
}

bool RedisClient::check_status(redisContext *ctx)
{
    redisReply *reply = (redisReply*)redisCommand(ctx, "PING");
    if(reply == NULL) return false;

    if(reply->type != REDIS_REPLY_STATUS) return false;
    if(strcasecmp(reply->str,"PONG") != 0) return false;
    freeReplyObject(reply);
    
    return true;
}

void RedisClient::push_cmd(string cmd)
{
    pthread_mutex_lock(&cmd_lock);
    client_cmd.push_back(cmd);
    pthread_mutex_unlock(&cmd_lock);    
    pthread_cond_signal(&cond);
}

void RedisClient::print_time()
{
    time_t t = time(NULL);
    char buftime[255] = {0};
    strftime(buftime, 255, "%Y-%m-%d %H:%M:%S", localtime(&t));
    printf("%s\n", buftime);
}

void RedisClient::parse_reply(redisReply* reply, bool is_first, 
    std::string* rp, std::vector<std::string>* keys, int* intera)
{
    if (!reply) {
        return;
    }

    switch (reply->type) {
        case REDIS_REPLY_ARRAY: {
            //返回一个数组，查看elements的值（数组个
            //数），通过element[index]的方式访问数组元素，每个数组元素是�
            //��个redisReply对象的指针
            //printf("array:\n");
            for (int i = 0; i < reply->elements; i++) {
                if (is_first && i == 0) {
                    parse_reply(reply->element[i], true, rp, NULL, NULL);
                } else {
                    parse_reply(reply->element[i], false, rp, keys, NULL);
                }
            }
        }break;
        
        case REDIS_REPLY_STRING: {//返回字符串，查看str,len字段
            std::string ss = get_str(reply->str, reply->len);
            if (is_first && rp) {
                *rp = ss;
            } else if (keys) {
                keys->push_back(ss);
            }
        }break;
        
        case REDIS_REPLY_INTEGER: {//返回整数，从integer字段获取值
            if (intera) {
                *intera = reply->integer;
            }
        }break;
        
        case REDIS_REPLY_NIL: {//没有数据返回
            printf("no data return\n");
        }break;
        
        case REDIS_REPLY_STATUS: {//表示状态，内容通过str字段查看，字符串长度是len字段
            std::string ss = get_str(reply->str, reply->len);
            if (is_first && rp) {
                *rp = ss;
            }
        }break;
        
        case REDIS_REPLY_ERROR: {//表示出错，查看出错信息，如上的str,len字段
            std::string ss = get_str(reply->str, reply->len);            
            if (is_first && rp) {
                *rp = ss;
            }
        }break;
        
        default:
            return;
    }
}

string RedisClient::get_str(char* data, int len)
{
    string ss;
    ss.append(data, len);
    return ss;
}

void RedisClient::start_do_cmd(int num)
{
    tid = new pthread_t[num];
    _start_thread = true;
    tid_num = num;
    for (int i = 0; i < tid_num; i++) {
        if (pthread_create(&tid[i], NULL, thread_do_cmd, this) == -1) {
            printf("pthread tid=%d create failed\n", tid[i]);
        }
    }
}

void RedisClient::stop_do_cmd()
{
    _start_thread = false;
    pthread_cond_signal(&cond);
    for (int i = 0; i < tid_num; i++) {
        if (pthread_join(tid[i], NULL) != 0) {
            printf("pthread tid=%d join failed\n", tid[i]);
        }
    }
}

void* RedisClient::thread_do_cmd(void* arg)
{
    RedisClient* client = (RedisClient*)arg;
    vector<string>& client_cmd =  client->client_cmd;
    while (client->_start_thread) {
        pthread_mutex_lock(&client->cmd_lock);
        pthread_cond_wait(&client->cond, &client->cmd_lock);
        vector<string> q = client_cmd;
        if (!q.empty()) {
            client_cmd.clear();
        }
        pthread_mutex_unlock(&client->cmd_lock);

        for (size_t i = 0; i < q.size(); i++) {
            string cmd = q[i];
            if (cmd != "") {
                string resp;
                if (REDIS_REPLY_ERROR == client->exec_cmd(cmd, &resp, NULL, NULL)) {
                    printf("client cmd=%s, respond=%s failed\n", cmd.c_str(), resp.c_str());
                }
                //printf("client cmd=%s, respond=%s successed\n", cmd.c_str(), resp.c_str());
            }
        }
    }
}


