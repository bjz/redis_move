#ifndef REDIS_MOVE_HPP
#define REDIS_MOVE_HPP

#include <string>
#include <queue>
#include <time.h>
#include <pthread.h>
#include <map>
#include <stdint.h>

#define REDIS_REPLY_STRING 1    // //返回字符串，查看str,len字段
#define REDIS_REPLY_ARRAY 2     //返回一个数组，查看elements的值（数组个数），通过element[index]的方式访问数组元素，每个数组元素是一个redisReply对象的指针
#define REDIS_REPLY_INTEGER 3   //返回整数，从integer字段获取值
#define REDIS_REPLY_NIL 4       //没有数据返回
#define REDIS_REPLY_STATUS 5    //表示状态，内容通过str字段查看，字符串长度是len字段
#define REDIS_REPLY_ERROR 6     //表示出错，查看出错信息，如上的str,len字段

struct redisReply;
struct redisContext;

using namespace std;
class RedisClient;

struct Client
{
    RedisClient* client1;
    RedisClient* client2;
};

class RedisClient
{
public:
    RedisClient(string ip, int port, string _passed, int timeout = 2000);
    virtual ~RedisClient();
public:
    int exec_cmd(int index, const string cmd, string* response, vector<string>* keys, int* intera);
    redisReply* exec_cmd(int index, const string cmd);
    void print_time();
    void push_cmd(string cmd);
    void start_do_cmd(int num, bool is_dest, Client* client);    
    void stop_do_cmd();    
    void push_key(std::vector<string> keys_v);
private:
    redisContext* create_context();
    bool check_status(redisContext *ctx);
    void parse_reply(redisReply* reply, bool is_first, 
        std::string* rp, std::vector<std::string>* keys, int* integer);
    string get_str(char* data, int len);
    static void* thread_do_cmd(void* arg);
    static void* thread_parse_key(void* arg);
private:
    int timeout;
    int server_port;
    string setver_ip;

    std::map<int, redisContext*> clients;
    pthread_mutex_t cmd_lock;
    pthread_cond_t cond;
    std::vector<string> client_cmd;
    bool _start_thread;
    pthread_t* tid;
    int tid_num;
    string passwd;

    pthread_mutex_t keys_lock;
    pthread_cond_t keys_cond;
    std::vector<std::vector<string> > all_keys;
public:
    int64_t total_cmd_num;
    int64_t total_keys_num;
    std::map<pthread_t, bool> thread_state;    
};

#endif // REDIS_MOVE_HPP
