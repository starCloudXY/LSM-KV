#include "SSTable.h"
#include <fstream>
#include <iostream>
#include <string.h>

SSTable4Compact::SSTable4Compact(SSTable *table)
{
    path = table->path;
    std::ifstream file(path, std::ios::binary);
    if(!file) {
        printf("Fail to open file %s", path.c_str());
        exit(-1);
    }
    timeStamp=table->TimeStamp();
    length = table->Number();
    // load from files
    file.seekg((table->indexes)[0].offset);
    for(auto it = (table->indexes).begin();;) {
        uint64_t key = it->key;
        uint32_t offset = (*it).offset;
        std::string val;
        if(++it == (table->indexes).end()) {
            file >> val;
            entries.push_back(SSNode(key, val));
            break;
        } else {
            uint32_t length = (*it).offset - offset;
            char *buf = new char[length + 1];
            buf[length] = '\0';
            file.read(buf, length);
            val = buf;
            delete[] buf;
            entries.push_back(SSNode(key, val));
        }
    }

    delete table;
}

void SSTable4Compact::merge(std::vector<SSTable4Compact> &tables)
{
    uint32_t size = tables.size();
    if(size == 1)
        return;
    uint32_t group = size/2;
    std::vector<SSTable4Compact> next;
    for(uint32_t i = 0; i < group; ++i) {
        next.push_back(merge2(tables[2*i], tables[2*i + 1]));
    }
    if(size % 2 == 1)
        next.push_back(tables[size - 1]);
    merge(next);
    tables = next;
}

SSTable4Compact SSTable4Compact::merge2(SSTable4Compact &a, SSTable4Compact &b)
{
    SSTable4Compact result;
    result.timeStamp = std::max(a.timeStamp,b.timeStamp);
    while((!a.entries.empty()) && (!b.entries.empty())) {
        uint64_t aKey = a.entries.front().key, bKey = b.entries.front().key;
        if(aKey > bKey) {
            result.entries.push_back(b.entries.front());
            b.entries.pop_front();
        } else if(aKey < bKey) {
            result.entries.push_back(a.entries.front());
            a.entries.pop_front();
        } else {
            result.entries.push_back(a.entries.front());
            a.entries.pop_front();
            b.entries.pop_front();
        }
    }
    while(!a.entries.empty()){
        result.entries.push_back(a.entries.front());
        a.entries.pop_front();
    }
    while(!b.entries.empty()){
        result.entries.push_back(b.entries.front());
        b.entries.pop_front();
    }
    return result;
}

std::vector<SSTable*> SSTable4Compact::save(const std::string &dir)
{
    std::vector<SSTable*> caches;
    SSTable4Compact newTable;
    uint64_t num = 0;
    while(!entries.empty()) {
        if(newTable.size + 12 + entries.front().val.size() >= MAX_TABLE_SIZE) {
            caches.push_back(newTable.saveSingle(dir, timeStamp, num++));
            newTable = SSTable4Compact();
        }
        newTable.add(entries.front());
        entries.pop_front();
    }
    if(newTable.length > 0) {
        caches.push_back(newTable.saveSingle(dir, timeStamp, num));
    }
    return caches;
}

void SSTable4Compact::add(const SSNode &entry)
{
    size += 12 + entry.val.size();
    length++;
    entries.push_back(entry);
}

SSTable *SSTable4Compact::saveSingle(const std::string &dir, const uint64_t &currentTime, const uint64_t &num)
{
    SSTable *newTable = new SSTable;

    char *buffer = new char[size];
    BloomFilter *filter = newTable->bloomFilter;

    *(uint64_t*)buffer=currentTime;
    newTable->setTimeStamp(currentTime);

    *(uint64_t*)(buffer+8) = length;
    newTable->setNumber(length);

    *(uint64_t*)(buffer + 16) = entries.front().key;
    newTable->setMin(entries.front().key);

    char *index = buffer + 10272;
    uint32_t offset = 10272 + length * 12;

    for(auto it = entries.begin(); it != entries.end(); ++it) {
        filter->add((*it).key);
        *(uint64_t*)index = (*it).key;
        index += 8;
        *(uint32_t*)index = offset;
        index += 4;

        (newTable->indexes).push_back(Index((*it).key, offset));
        uint32_t strLen = ((*it).val).size();
        uint32_t newOffset = offset + strLen;
        if(newOffset > size) {
            printf("Buffer Overflow!!!\n");
            exit(-1);
        }
        memcpy(buffer + offset, ((*it).val).c_str(), strLen);
        offset = newOffset;
    }

    *(uint64_t*)(buffer + 24) = entries.back().key;
    newTable->setMax(entries.back().key);
    filter->save2Buffer(buffer + 32);

    std::string filename = dir + "/" + std::to_string(currentTime) + "-" + std::to_string(num) + ".sst";
    newTable->path = filename;
    std::ofstream outFile(filename, std::ios::binary | std::ios::out);
    outFile.write(buffer, size);

    delete[] buffer;
    outFile.close();
    return newTable;
}

SSTable::SSTable(const std::string &dir)
{
    path = dir;
    std::ifstream file(dir, std::ios::binary);
    if(!file) {
        printf("Fail to open file %s", dir.c_str());
        exit(-1);
    }
    // load header
    file.read((char*)&header[0], 8);
    file.read((char*)&header[1], 8);
    file.read((char*)&header[2], 8);
    file.read((char*)&header[3], 8);
    // load bloom filter
    char *filterBuf = new char[FILTER_SIZE/8];
    file.read(filterBuf, FILTER_SIZE/8);
    bloomFilter = new BloomFilter(filterBuf);
    uint64_t length =  Number();

    char *indexBuf = new char[length * 12];
    file.read(indexBuf, length * 12);
    for(uint32_t i = 0; i < length; ++i) {
        indexes.push_back(Index(*(uint64_t*)(indexBuf + 12*i), *(uint32_t*)(indexBuf + 12*i + 8)));
    }

    delete[] filterBuf;
    delete[] indexBuf;
    file.close();

}
int SSTable::get_in_cache(const uint64_t &key)
{
    if(!bloomFilter->contains(key))return -1;
    return find(key, 0, indexes.size() - 1);
}
int SSTable::get(const uint64_t &key)
{
    return find(key, 0, indexes.size() - 1);
}

int SSTable::find(const uint64_t &key, int start, int end)
{
    if(start > end)
        return -1;
    if(start == end) {
        if(indexes[start].key == key)
            return start;
        else
            return -1;
    }
    int mid = (start + end) / 2;
    if(indexes[mid].key == key)
        return mid;
    else if(indexes[mid].key < key)
        return find(key, mid + 1, end);
    else
        return find(key, start, mid - 1);
}

bool TimeCompare(SSTable *a, SSTable *b)
{
    return a->TimeStamp() > b->TimeStamp();
}

bool tableTimeCompare(SSTable4Compact &a, SSTable4Compact &b)
{
    return a.timeStamp > b.timeStamp;
}

void SSTable4Compact::handleDelete(SSTable4Compact &table) {
    auto t= table.entries.begin();
    while(t!=table.entries.end()) {
        if ((*t).val == "~DELETED~"){
            auto tmp =t;
            t++;
            table.entries.erase(tmp);
        }
        else t++;
    }
}