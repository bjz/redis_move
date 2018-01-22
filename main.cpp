#include "redis_move.hpp"
#include <string.h>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <getopt.h>

struct option opts[] = {
    {"host1", required_argument, NULL, 's'},
    {"host2", required_argument, NULL, 'd'},
    {"port1", required_argument, NULL, 'S'},
    {"port2", required_argument, NULL, 'D'},
    {"time",  required_argument, NULL, 't'},
    {"passwd1", required_argument, NULL, 'P'},
    {"passwd2", required_argument, NULL, 'p'},
    {"threads", required_argument, NULL, 'n'},
    {"count", required_argument, NULL, 'c'},
    {"help",    no_argument, NULL, 'h'}
};

bool check_args(string s1, string s2, string s3, string s4, int a1, int a2, int a3, int a4, int a5) 
{
    if (s1.empty()) {
        printf("source redis host cannot empty\n");
        return false;        
    } else if (s2.empty()) {    
        printf("dest redis host cannot empty\n");
        return false;        
    }/* else if (s3.empty()) {
        printf("source passwd redis host cannot empty\n");
        return false;        
    } else if (s4.empty()) {
        printf("dest passwd redis host cannot empty\n");
        return false;        
    } */else if (a1 <= 0) {    
        printf("source redis port error\n");
        return false;        
    } else if (a2 <= 0) {
        printf("dest redis port error\n");
        return false;        
    } else if (a3 <= 0) {    
        printf("interval error\n");
        return false;        
    } else if (a4 <= 0) {
        printf("thread num error\n");
        return false;        
    } else if (a5 <= 0) {
        printf("every time return keys must > 0");
    }

    return true;
}

void help()
{
    printf("option:\n\n"
        "-s --host1: source redis host\n"
        "-d --host2: dest redis host\n"
        "-S --port1: source redis port\n"
        "-D --port2: dest redis port\n"
        "-t --time: interval time(us), defult 1000us\n"
        "-h --help: option help\n"
        "-p --passwd1: source redis passwd\n"
        "-P --passwd2: dest redis passwd\n"
        "-n --threads: start threads num to process, default thread num is 1\n"
        "-c --count: every time return keys count, defult count is 100\n"
        "\n\n"
        "example: ./redis_move -s 192.168.1.12 -S 1000 -d 192.168.1.133 -D 2000 -t 1000 -n 4 -c 100 -p *** -P *** \n\n");
}

int main(int argc, char **argv) {
    
    int opt = 0;
    string src_hostname, dest_hostname, passwd1, passwd2, count_str = "100";
    int src_port = 0, dest_port = 0;
    int timevalue = 1000, thread_num = 1, count = 100;
    bool is_help = false;
    while ((opt = getopt_long(argc, argv, "s:d:S:D:t:P:p:n:c:h", opts, NULL)) != -1) {
        switch (opt) {
            case 's':
                src_hostname = optarg;
            break;                        
            case 'c':
                count = atoi(optarg);
                count_str = optarg;
            break;            
            case 'd':
                dest_hostname = optarg;
            break;
            case 'S':
                src_port = atoi(optarg);
            break;
            case 'D':
                dest_port = atoi(optarg);
            break;
            case 't':
                timevalue = atoi(optarg);
            break;
            case 'p':
                passwd1 = optarg;
            break;
            case 'P':
                passwd2 = optarg;
            break;
            case 'n':
                thread_num = atoi(optarg);
            break;            
            case 'h':
                is_help = true;
            break;
            default:
                printf("option error\n");
                break;
        }
    }
    
    if (is_help) {
        help();
        return 0;
    }

    if (!check_args(src_hostname, dest_hostname, passwd1, passwd2, src_port, dest_port, timevalue, thread_num, count)) {
        printf("please: ./redis_move -h\n");
        return 0;
    }
    
    RedisClient *src_client = new RedisClient(src_hostname, src_port, passwd1, 3000);// time is 3s    
    RedisClient *dest_client = new RedisClient(dest_hostname, dest_port, passwd2, 3000);// time is 3s
    dest_client->start_do_cmd(thread_num);
    src_client->print_time();
 
    std::string rp = "";
    while (rp != "0") {
        usleep(timevalue);
        if (rp == "") {
            rp = "0";
        }
        
        std::vector<std::string> keys;
        std::string cmd = "SCAN " + rp + " COUNT " + count_str;
        printf("str=%s\n", cmd.c_str());
        if (REDIS_REPLY_ERROR == src_client->exec_cmd(cmd, &rp, &keys, NULL)) {//get all keys
            printf("error=%s, cmd=%s\n", rp.c_str(), cmd.c_str());
            goto failed;
        }
        printf("keys rp=%s\n", rp.c_str());

        for (int i = 0; i < keys.size(); i++) {
            int type = -1;
            string respond = "";
            string key = keys[i];
            cmd = "TYPE " + key;
            if ((type = src_client->exec_cmd(cmd, &respond, NULL, NULL)) == REDIS_REPLY_ERROR) {
                printf("error=%s, cmd=%s\n", respond.c_str(), cmd.c_str());
                continue;
            }

            if (type != REDIS_REPLY_STATUS) {
                printf("error=%s, cmd=%s\n", respond.c_str(), cmd.c_str());
                continue;
            }
            
            if (respond == "set") {
                //cmd = "SMEMBERS " + key;//usr sscan replace smembers
                string rq = "";
                while (rq != "0") {
                    usleep(timevalue);
                    if (rq == "") {
                        rq = "0";
                    }
                    cmd = "SSCAN " + key + " " + rq + " COUNT 100";
                    //printf("str=%s\n", cmd.c_str());
                    vector<string> members;
                    printf("set rq=%s\n", rq.c_str());
                    if (REDIS_REPLY_ERROR ==src_client->exec_cmd(cmd, &rq, &members, NULL)) {
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
                if (REDIS_REPLY_ERROR == src_client->exec_cmd(cmd, &value, NULL, NULL)) {
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

failed:    
    dest_client->stop_do_cmd();
    
    src_client->print_time();
    return 0;
}


