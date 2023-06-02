#ifndef DATASTRUCTS_H
#define DATASTRUCTS_H

#include <stdint.h>
#include <string>

struct SSNode
{
    uint64_t key;
    std::string val;
    SSNode(uint64_t k, std::string v): key(k), val(v) {}
};
struct Config{
    int max=0;
    bool type=0;
};

struct Index
{
    uint64_t key;
    uint32_t offset;
    Index(uint64_t k = 0, uint32_t o = 0): key(k), offset(o) {}
};


struct SkipNode
{
    SkipNode **forward;//存储指向下一层节点的指针
    uint64_t key;
    std::string val;
    SkipNode(int lvl,uint64_t searchKey,std::string val){
        forward = new SkipNode *[lvl];
        for(int i=0;i<lvl;i++){
            forward[i]= nullptr;
        }
        this->val=val;
        this->key=searchKey;
    }
    ~SkipNode(){
        delete []forward;
    }
};




#endif // DATASTRUCTS_H
