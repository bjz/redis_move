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

RedisClient::RedisClient(string ip, int port, string _passwd, int _timeout)
{
    timeout = _timeout;
    server_port = port;
    setver_ip = ip;
    _start_thread = false;
    tid = NULL;
    tid_num = 0;
    total_cmd_num = 0;
    total_keys_num = 0;
    passwd = _passwd;
    pthread_mutex_init(&cmd_lock, NULL);
    pthread_cond_init(&cond, NULL);

    pthread_mutex_init(&keys_lock, NULL);
    pthread_cond_init(&keys_cond, NULL);
}

RedisClient::~RedisClient()
{
    stop_do_cmd();

    for (int i = 0; i < tid_num; i++) {
        redisContext *ctx = clients[i];
        redisFree(ctx);
    }
    clients.clear();
    
    pthread_mutex_lock(&cmd_lock);
    pthread_mutex_unlock(&cmd_lock);
    pthread_mutex_destroy(&cmd_lock);
    pthread_cond_destroy(&cond);

    pthread_mutex_destroy(&keys_lock);
    pthread_cond_destroy(&keys_cond);
}

int RedisClient::exec_cmd(int index, const string cmd, string* response, vector<std::string>* keys, int* intera)
{
    redisReply *reply = exec_cmd(index, cmd);
    if (!reply) {
        printf("error: reply is null\n");
        return REDIS_REPLY_ERROR;
    }
    parse_reply(reply, true, response, keys, intera);
    int type = reply->type;
    freeReplyObject(reply);

    return type;
}

redisReply* RedisClient::exec_cmd(int index, const string cmd)
{
    redisContext *ctx = clients[index];
    if(ctx == NULL) return NULL;

    redisReply *reply = (redisReply*)redisCommand(ctx, cmd.c_str());

    return reply;
}

redisContext* RedisClient::create_context()
{
    struct timeval tv;
    tv.tv_sec = timeout / 1000;
    tv.tv_usec = (timeout % 1000) * 1000;
    redisContext *ctx = redisConnectWithTimeout(setver_ip.c_str(), server_port, tv);
    if(ctx == NULL || ctx->err != 0)
    {
        if(ctx != NULL) {
            printf("create free\n");
            redisFree(ctx);
            return NULL;
        }        
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
    }

    return ctx;
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
    total_cmd_num++;
    client_cmd.push_back(cmd);
    pthread_mutex_unlock(&cmd_lock);    
    pthread_cond_signal(&cond);
}

void RedisClient::push_key(std::vector<string> keys_v)
{
    pthread_mutex_lock(&keys_lock);
    all_keys.push_back(keys_v);
    total_keys_num += keys_v.size();
    pthread_mutex_unlock(&keys_lock);
    pthread_cond_signal(&keys_cond);
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

void RedisClient::start_do_cmd(int num, bool is_dest, Client* client)
{
    _start_thread = true;
    if (is_dest) {
        tid_num = num;
    } else {
        tid_num = num + 1;
    }
            
    for (int i = 0; i< tid_num; i++) {
        clients[i] = create_context();
        if (clients[i] == NULL) {
            printf("create context failed\n");
            exit(0);
        }
    }
    
    tid = new pthread_t[num];
    for (int i = 0; i < num; i++) {
        if (pthread_create(&tid[i], NULL, 
            (is_dest ? thread_do_cmd : thread_parse_key), (is_dest ? (void*)this : (void*)client)) == -1) {
            printf("pthread tid=%d create failed\n", tid[i]);
        } else {
            thread_state[tid[i]] = true;
        }
    }   
}

void RedisClient::stop_do_cmd()
{
    while ((!client_cmd.empty()) || (!all_keys.empty())) {
        printf("cmd=%d, all_keys=%d\n", client_cmd.size(), all_keys.size());
        usleep(500);
        pthread_cond_signal(&keys_cond);
        pthread_cond_signal(&cond);        
    }
    _start_thread = false;

    for (std::map<pthread_t, bool>::iterator it = thread_state.begin(); it != thread_state.end(); it++) {
        while (it->second) {
            usleep(500);
            pthread_cond_signal(&cond);
            pthread_cond_signal(&keys_cond);
        }
    }
    
    pthread_cond_signal(&keys_cond);
    pthread_cond_signal(&cond);    
}

void* RedisClient::thread_parse_key(void* arg)
{
    printf("parse thread[%d] run start\n", pthread_self());
    pthread_detach(pthread_self());

    Client* client_all = (Client*)arg;
    RedisClient* src_client = client_all->client1;    
    RedisClient* dest_client = client_all->client2;
    
    pthread_mutex_lock(&src_client->keys_lock);
    static int parse_thread_index = 0;
    parse_thread_index++;
    pthread_mutex_unlock(&src_client->keys_lock);
    int index = parse_thread_index;
    
    while (src_client->_start_thread) {
        pthread_mutex_lock(&src_client->keys_lock);
        pthread_cond_wait(&src_client->keys_cond, &src_client->keys_lock);
        std::vector<string> keys_v;
        if (!src_client->all_keys.empty()) {
            keys_v = src_client->all_keys[0];
            src_client->all_keys.erase(src_client->all_keys.begin());
        }
        pthread_mutex_unlock(&src_client->keys_lock);
        
        for (int i = 0; i < keys_v.size(); i++) {
            int type = -1;
            string respond = "";
            string key = keys_v[i];
            string cmd = "TYPE " + key;
            if ((type = src_client->exec_cmd(index, cmd, &respond, NULL, NULL)) != REDIS_REPLY_STATUS) {
                printf("error=%s, cmd=%s\n", respond.c_str(), cmd.c_str());
                continue;
            }
            
            if (respond == "set") {
                //cmd = "SMEMBERS " + key;//usr sscan replace smembers
                string rq = "";
                while (rq != "0") {
                    if (rq == "") {
                        rq = "0";
                    }
                    cmd = "SSCAN " + key + " " + rq + " COUNT 100";
                    //printf("sscan str=%s\n", cmd.c_str());
                    vector<string> members;
                    printf("set rq=%s\n", rq.c_str());
                    if (REDIS_REPLY_ARRAY !=src_client->exec_cmd(index, cmd, &rq, &members, NULL)) {
                        printf("error=%s, cmd=%s\n", rq.c_str(), cmd.c_str());
                        continue;
                    }
                    for (int i = 0; i < members.size(); i++) {
                        std::string value = members[i];
                        string set_cmd = "SADD " + key + " " + value;
                        dest_client->push_cmd(set_cmd);
                    }
                }
            } else if (respond == "string") {
                cmd = "GET " + key;
                string value;
                if (REDIS_REPLY_STRING != src_client->exec_cmd(index, cmd, &value, NULL, NULL)) {
                    printf("error=%s, cmd=%s\n", value.c_str(), cmd.c_str());
                    continue;
                }
                string set_cmd = "SET " + key + " " + value;
                dest_client->push_cmd(set_cmd);
            } else if (respond == "hash") {

            } else if (respond == "list") {

            } else if (respond == "zset") {

            } else {

            }
        }        
    }

    src_client->thread_state[pthread_self()] = false;    
    printf("parse thread[%d] run end\n", pthread_self());
}

void* RedisClient::thread_do_cmd(void* arg)
{
    printf("do cmd thread[%d] run start\n", pthread_self());
    pthread_detach(pthread_self());

    RedisClient* client = (RedisClient*)arg;    

    pthread_mutex_lock(&client->cmd_lock);
    static int thread_index = 0;
    thread_index++;
    pthread_mutex_unlock(&client->cmd_lock);
    
    int index = thread_index;
    vector<string>& client_cmd =  client->client_cmd;
    vector<string> q;
    while (client->_start_thread) {
        pthread_mutex_lock(&client->cmd_lock);
        pthread_cond_wait(&client->cond, &client->cmd_lock);
        if (client_cmd.size() > 50) {
            q.assign(client_cmd.begin(), client_cmd.begin() + 50);
            client_cmd.erase(client_cmd.begin(), client_cmd.begin() + 50);
        } else {
            //q = client_cmd;
            q.assign(client_cmd.begin(), client_cmd.end());
            client_cmd.clear();
        }
        pthread_mutex_unlock(&client->cmd_lock);

        
        for (int i = 0; i < q.size(); i++) {                        
            string cmd = q[i];
            if (cmd != "") {
                string resp;
                int type = client->exec_cmd(index - 1, cmd, &resp, NULL, NULL);
                if (REDIS_REPLY_STRING != type && type != REDIS_REPLY_INTEGER && type != REDIS_REPLY_STATUS) {
                    printf("client cmd=%s, respond=%s failed\n", cmd.c_str(), resp.c_str());
                }
                //printf("client cmd=%s, respond=%s successed\n", cmd.c_str(), resp.c_str());
            }
        }
        
        q.clear();
    }
    client->thread_state[pthread_self()] = false;
    printf("do cmd thread[%d] run end\n", pthread_self());
}


