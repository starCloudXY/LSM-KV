#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <cstdint>
#include <vector>
#include <random>
#include <string>

#include "DataStructs.h"
#include "SSTable.h"
#define HEADER 32
#define BF 10240
#define KEY 8
#define OFFSET 4
#define MAX_LEVEL 256

class SkipList
{
private:
    SkipNode *header;
    SkipNode *tail;
    uint64_t listLength;
    // 转换为SSTable后的大小，无数据时为10272B
    uint64_t listSize;
    int _level;
public:
    SkipList():_level(0),listSize(10272),listLength(0){
        tail = new SkipNode(MAX_LEVEL, std::numeric_limits<uint64_t>::max(),"");
        header = new SkipNode(MAX_LEVEL, -1,"");
        for (int i = 0; i < MAX_LEVEL; i++)header->forward[i] = tail;
    }
    SkipList(std::vector<SSNode> entrys);
    ~SkipList();
    uint64_t length();
    uint64_t size();
    std::string *get(const uint64_t &key) const;

    int randomValue();
    // 返回表中原来是否有该key
    bool put(const uint64_t &key, const std::string &val);

    // 返回是否成功删除（原表中是否有该key）
    bool remove(const uint64_t &key);
    SkipNode* getListHead();
    SSTable *save2SSTable(const std::string &dir, const uint64_t &currentTime);
};

#endif // SKIPLIST_H
