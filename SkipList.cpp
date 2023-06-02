#include "SkipList.h"
#include "BloomFilter.h"
#include <fstream>
#include <stdlib.h>
#include <chrono>
#include <string.h>

SkipList::SkipList(std::vector<SSNode> entrys)
{
    for(auto it = entrys.begin(); it != entrys.end(); ++it)
        put((*it).key, (*it).val);
}

SkipList::~SkipList()
{
    SkipNode *current = header->forward[0];
    while (current != nullptr) {
        SkipNode *next = current->forward[0];
        delete current;
        current = next;
    }
    delete header;
}

uint64_t SkipList::length()
{
    return listLength;
}

uint64_t SkipList::size()
{
    return listSize;
}

std::string *SkipList::get(const uint64_t &key) const
{
    int len = 0;
    if (header == nullptr)return 0;
    SkipNode *x = header;
    for (int i = _level - 1; i >= 0; i--) {
        while (x->forward[i]->key < key) {
            x = x->forward[i];
            len++;
        }
    }
    x = x->forward[0];
    if (x->key == key)return &(x->val);
    else return nullptr;
}

// 返回表中原来是否有该key
bool SkipList::put(const uint64_t &key, const std::string &val)
{
    SkipNode *update[MAX_LEVEL];
    SkipNode *x = header;
    for (int i = _level - 1; i >= 0; i--) {
        while (x->forward[i] != nullptr && x->forward[i]->key < key) {
            x = x->forward[i];
        }
        update[i] = x;
    }
    x = x->forward[0];
    if (x->key == key) {
        x->val = val;
        return true;
    }
    else {
        int lvl = randomValue();
        if (lvl > _level) {
            for (int i = _level; i < lvl; i++)update[i] = header;
            _level = lvl;
        }
        SkipNode *new_node = new SkipNode(lvl, key, val);
        for (int i = lvl - 1; i >= 0; i--) {
            new_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = new_node;
        }
    }
    ++listLength;
    // 加一个key、一个offset、一个string的大小
    listSize += 12 + val.size();
    return false;
}

// 返回是否成功删除（原表中是否有该key）
bool SkipList::remove(const uint64_t &key)
{
    return put(key, "~DELETED~");
}
int SkipList::randomValue() {
    int lvl = 1;
    std::random_device seed;//硬件生成随机数种子
    std::ranlux48 engine(seed());//利用种子生成随机数引擎
    while (rand()%1 && lvl < MAX_LEVEL) {
        lvl++;
    }
    return lvl;
}
SkipNode *SkipList::getListHead()
{
    SkipNode *cur = header;
    return cur->forward[0];
}

SSTable *SkipList::save2SSTable(const std::string &dir, const uint64_t &currentTime)
{
    SSTable *cache = new SSTable;
    SkipNode *cur = getListHead();
    char *buffer = new char[listSize];
    BloomFilter *filter = cache->bloomFilter;

    *(uint64_t*)buffer = currentTime;
    cache->setTimeStamp(currentTime);

    *(uint64_t*)(buffer + 8) = listLength;
    cache->setNumber(listLength);

    *(uint64_t*)(buffer + 16) = cur->key;
    cache->setMin(cur->key);

    char *index = buffer + HEADER+BF;
    uint32_t offset = HEADER+BF+ listLength * (KEY+OFFSET);
    while(true) {
        filter->add(cur->key);
        *(uint64_t*)index = cur->key;
        index += KEY;
        *(uint32_t*)index = offset;
        index += OFFSET;

        (cache->indexes).push_back(Index(cur->key, offset));
        uint32_t strLen = (cur->val).size();
        uint32_t newOffset = offset + strLen;
        if(newOffset > listSize) {
            printf("Buffer Overflow!!!\n");
            exit(-1);
        }
        memcpy(buffer + offset, (cur->val).c_str(), strLen);
        offset = newOffset;
        if(cur->forward[0])
            cur = cur->forward[0];
        else
            break;
    }
    *(uint64_t*)(buffer + 24) = cur->key;
    cache->setMax(cur->key);
    filter->save2Buffer(buffer + 32);

    std::string filename = dir + "/" + std::to_string(currentTime) + ".sst";
    cache->path = filename;
    std::ofstream outFile(filename, std::ios::binary | std::ios::out);
    outFile.write(buffer, listSize);

    delete[] buffer;
    outFile.close();
    return cache;
}
