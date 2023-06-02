#ifndef SSTABLE_H
#define SSTABLE_H

#include "BloomFilter.h"
#include "DataStructs.h"
#include <string>
#include <vector>
#include <list>

#define MAX_TABLE_SIZE 2097152


class SSTable
{
public:
    uint64_t header[4];

    const uint64_t TimeStamp(){return header[0];}
    const uint64_t Number(){return header[1];}
    const uint64_t Min(){return header[2];}
    const uint64_t Max(){return header[3];}
    void setTimeStamp(uint64_t num){
        header[0]=num;
    }
    void setNumber(uint64_t num){
        header[1]=num;
    }
    void setMin(uint64_t num){
        header[2]=num;
    }
    void setMax(uint64_t num){
        header[3]=num;
    }

    BloomFilter *bloomFilter;
    std::vector<Index> indexes;
    std::string path;

    ~SSTable(){delete bloomFilter;}
    SSTable(): bloomFilter(new BloomFilter()) {}
    SSTable(const std::string &dir);

    int get(const uint64_t &key);
    int get_in_cache(const uint64_t &key);
    int find(const uint64_t &key, int start, int end);
};

class SSTable4Compact
{
public:
    uint64_t timeStamp;
    std::string path;
    uint64_t size;
    uint64_t length;
    std::list<SSNode> entries;


    SSTable4Compact(SSTable *table);
    SSTable4Compact(): size(10272), length(0) {}
    static void merge(std::vector<SSTable4Compact> &tables);
    static SSTable4Compact merge2(SSTable4Compact &a, SSTable4Compact &b);

    std::vector<SSTable*> save(const std::string &dir);
    SSTable *saveSingle(const std::string &dir, const uint64_t &currentTime, const uint64_t &num);
    static void handleDelete(SSTable4Compact &table);
    void add(const SSNode &entry);

};

bool TimeCompare(SSTable *a, SSTable *b);
bool tableTimeCompare(SSTable4Compact &a, SSTable4Compact &b);


#endif // SSTABLE_H
