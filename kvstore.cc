#include "kvstore.h"
#include "utils.h"
#include <string>
#include <algorithm>
#include <fstream>
#include <iostream>


KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    if(dir[dir.length()] == '/')
        dataDir = dir.substr(0, dir.length() - 1);
    else
        dataDir = dir;
    currentTime = 0;
    // load tablelist from existed SSTables
    if (utils::dirExists(dataDir)) {
        std::vector<std::string> levelNames;
        int levelNum = utils::scanDir(dataDir, levelNames);
        if (levelNum > 0) {
            for(int i = 0; i < levelNum; ++i) {
                std::string levelName = "level-" + std::to_string(i);
                // check if the level directory exists
                if(std::count(levelNames.begin(), levelNames.end(), levelName) == 1) {
                    tablelist.push_back(std::vector<SSTable*>());

                    std::string levelDir = dataDir + "/" + levelName;
                    std::vector<std::string> tableNames;
                    int tableNum = utils::scanDir(levelDir, tableNames);
                    for(int j = 0; j < tableNum; ++j) {
                        SSTable* curCache = new SSTable(levelDir + "/" + tableNames[j]);
                        uint64_t curTime = curCache->TimeStamp();
                        tablelist[i].push_back(curCache);
                        if(curTime > currentTime)
                            currentTime = curTime;
                    }
                    // make sure the timeStamp of tablelist is decending
                    std::sort(tablelist[i].begin(), tablelist[i].end(), TimeCompare);
                } else
                    break;
            }
        } else {
            utils::mkdir((dataDir + "/level-0").c_str());
            tablelist.push_back(std::vector<SSTable*>());
        }
    } else {
        utils::mkdir(dataDir.c_str());
        utils::mkdir((dataDir + "/level-0").c_str());
        tablelist.push_back(std::vector<SSTable*>());
    }
    currentTime++;
    memTable = new SkipList();
    std::ifstream file("./default.conf", std::ios::binary);
    char s[10];
    int num,maxlevel;
    while(file>>num>>maxlevel>>s){
        configs[num].max=maxlevel;
        if(!std::strcmp(s,"Tiering")){
            configs[num].type=Tiering;
        }
        else if(!std::strcmp(s,"Leveling")){
            configs[num].type=Leveling;
        }
    }
}

KVStore::~KVStore()
{
    if(memTable->length() > 0)
        memTable->save2SSTable(dataDir + "/level-0", currentTime++);
    delete memTable;
    for(auto it1 = tablelist.begin(); it1 != tablelist.end(); ++it1) {
        for(auto it2 = (*it1).begin(); it2 != (*it1).end(); ++it2)
            delete (*it2);
    }
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if(memTable->size() + s.size() + 12 < MAX_TABLE_SIZE) {
        memTable->put(key, s);
        return;
    }
    tablelist[0].push_back(memTable->save2SSTable(dataDir + "/level-0", currentTime++));
    delete memTable;
    memTable = new SkipList;
    std::sort(tablelist[0].begin(), tablelist[0].end(), TimeCompare);
    uint32_t levelNum = tablelist.size();
    for(uint32_t i = 0; i < levelNum; ++i) {
        if(tablelist[i].size() >configs[i].max)
            compactLevel(i);
        else
            break;
    }
    memTable->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
//
std::string KVStore::get_without_cache(uint64_t key)
{
    std::string *val = memTable->get(key);
    if(val) {
        if(*val == "~DELETED~")
            return "";
        return *val;
    }
    // cannot find in memTable, try to find in SSTables
    int levelNum = tablelist.size();
    for(int i = 0; i < levelNum; ++i) {
        for(auto it = tablelist[i].begin(); it != tablelist[i].end(); ++it) {
            SSTable searchTable = SSTable((*it)->path);
            // check if the key is in the range of the sstablecache
            if(key <= (searchTable).Max() && key >= ((searchTable).Min())) {
                int pos = searchTable.get(key);
                if(pos < 0)
                    continue;
                std::ifstream file(searchTable.path, std::ios::binary);
                if(!file) {
                    printf("Lost file: %s", ((searchTable).path).c_str());
                    exit(-1);
                }

                std::string value;
                uint32_t length, offset = ((searchTable).indexes)[pos].offset;
                file.seekg(offset);

                // check if it is the last entry
                if((unsigned long)pos == ((searchTable).indexes).size() - 1) {
                    file >> value;
                } else {
                    uint32_t nextOffset = ((searchTable).indexes)[pos + 1].offset;
                    length = nextOffset - offset;
                    char *result = new char[length + 1];
                    result[length] = '\0';
                    file.read(result, length);
                    value = result;
                    delete[] result;
                }
                file.close();
                if(value == "~DELETED~")
                    return "";
                else
                    return value;
            }

        }
    }
    return "";
}
std::string KVStore::get_in_cache(uint64_t key)
{
    std::string *val = memTable->get(key);
    if(val) {
        if(*val == "~DELETED~")
            return "";
        return *val;
    }
    // cannot find in memTable, try to find in SSTables
    int levelNum = tablelist.size();
    for(int i = 0; i < levelNum; ++i) {
        for(auto it = tablelist[i].begin(); it != tablelist[i].end(); ++it) {
            // check if the key is in the range of the sstablecache
            if(key <= (*it)->Max() && key >= ((*it)->Min())) {
                int pos = (*it)->get_in_cache(key);
                if(pos < 0)
                    continue;
                std::ifstream file((*it)->path, std::ios::binary);
                if(!file) {
                    printf("Lost file: %s", ((*it)->path).c_str());
                    exit(-1);
                }

                std::string value;
                uint32_t length, offset = ((*it)->indexes)[pos].offset;
                file.seekg(offset);

                // check if it is the last entry
                if((unsigned long)pos == ((*it)->indexes).size() - 1) {
                    file >> value;
                } else {
                    uint32_t nextOffset = ((*it)->indexes)[pos + 1].offset;
                    length = nextOffset - offset;
                    char *result = new char[length + 1];
                    result[length] = '\0';
                    file.read(result, length);
                    value = result;
                    delete[] result;
                }
                file.close();
                if(value == "~DELETED~")
                    return "";
                else
                    return value;
            }

        }
    }
    return "";
}
std::string KVStore::get(uint64_t key)
{
    std::string *val = memTable->get(key);
    if(val) {
        if(*val == "~DELETED~")
            return "";
        return *val;
    }
    // cannot find in memTable, try to find in SSTables
    int levelNum = tablelist.size();
    for(int i = 0; i < levelNum; ++i) {
        for(auto it = tablelist[i].begin(); it != tablelist[i].end(); ++it) {
            // check if the key is in the range of the sstablecache
            if(key <= (*it)->Max() && key >= ((*it)->Min())) {
                int pos = (*it)->get_in_cache(key);
                if(pos < 0)
                    continue;
                std::ifstream file((*it)->path, std::ios::binary);
                if(!file) {
                    printf("Lost file: %s", ((*it)->path).c_str());
                    exit(-1);
                }

                std::string value;
                uint32_t length, offset = ((*it)->indexes)[pos].offset;
                file.seekg(offset);

                // check if it is the last entry
                if((unsigned long)pos == ((*it)->indexes).size() - 1) {
                    file >> value;
                } else {
                    uint32_t nextOffset = ((*it)->indexes)[pos + 1].offset;
                    length = nextOffset - offset;
                    char *result = new char[length + 1];
                    result[length] = '\0';
                    file.read(result, length);
                    value = result;
                    delete[] result;
                }
                file.close();
                if(value == "~DELETED~")
                    return "";
                else
                    return value;
            }

        }
    }
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string val = get(key);
    if(val == "")
        return false;
    put(key, "~DELETED~");
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    delete memTable;
    memTable = new SkipList();
    for(auto it1 = tablelist.begin(); it1 != tablelist.end(); ++it1) {
        for(auto it2 = (*it1).begin(); it2 != (*it1).end(); ++it2)
            delete (*it2);
    }
    tablelist.clear();
    tablelist.push_back(std::vector<SSTable*>());
}



void KVStore::compactLevel(uint32_t level)
{
    ::uint64_t min=UINT64_MAX,max=0;
    std::vector<SSTable4Compact>tmp;
    //Tiering
    if(configs[level].type == Tiering) {
        for (auto t: tablelist[level]) {
            min = min < t->Min() ? min : t->Min();
            max = max > t->Max() ? max : t->Max();
            tmp.push_back(SSTable4Compact(t));
        }
        tablelist[level].clear();
    }
    else{
        int num = configs[level].max;
        if(num<=0)return;

        auto it  = tablelist[level].begin();
        for(uint64_t i = 0; i < num; ++i)
            ++it;
        int timestamp_divide = (*it)->TimeStamp();
        while(it != tablelist[level].begin()) {
            if((*it)->TimeStamp() > timestamp_divide)
                break;
            --it;
        }
        if((*it)->TimeStamp() > timestamp_divide)
            ++it;
        while(it!=tablelist[level].end()){
            min = min < (*it)->Min()?min : (*it)->Min();
            max = max > (*it)->Max()?max : (*it)->Max();
            tmp.push_back(SSTable4Compact(*it));
            tablelist[level].erase(it);
        }
    }
    level++;
    if(level<tablelist.size()){
        if(configs[level].type==Tiering){
            for (auto t=tablelist[level].begin();t!=tablelist[level].end();) {
                    tmp.push_back(SSTable4Compact(*t));
                    t = tablelist[level].erase(t);
            }
        }
        if(configs[level].type==Leveling){
            for (auto t=tablelist[level].begin();t!=tablelist[level].end();) {
                if((*t)->Max()>min||(*t)->Min()<max){
                    tmp.push_back(SSTable4Compact(*t));
                    t = tablelist[level].erase(t);
                }
                else t++;
            }
        }
    }
    else {
        utils::mkdir((dataDir + "/level-" + std::to_string(level)).c_str());
        tablelist.push_back(std::vector<SSTable*>());
    }
    sort(tmp.begin(),tmp.end(),tableTimeCompare);

    for(auto it = tmp.begin(); it != tmp.end(); ++it)
        utils::rmfile((*it).path.c_str());
    sort(tmp.begin(),tmp.end(), tableTimeCompare);
    SSTable4Compact::merge(tmp);
    if(level==tablelist.size()-1){
        SSTable4Compact::handleDelete(tmp[0]);
    };
    std::vector<SSTable *> newTables = tmp[0].save(dataDir + "/level-" + std::to_string(level));
    for(auto it = newTables.begin(); it != newTables.end(); ++it) {
        tablelist[level].push_back(*it);
    }
    sort(tablelist[level].begin(), tablelist[level].end(), TimeCompare);
}
