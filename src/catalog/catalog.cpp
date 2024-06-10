#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  LOG(INFO)<< "into CatalogMeta::SerializeTo";
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  LOG(INFO)<< "into CatalogMeta::DeserializeFrom";
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
// 与序列化函数对照
uint32_t CatalogMeta::GetSerializedSize() const {
  LOG(INFO)<< "into CatalogMeta::GetSerializedSize";
  uint32_t size = 0;
  size += 4;  // magic number
  size += 4;  // table_meta_pages_.size() 所占位数
  size += 4;  // index_meta_pages_.size() 所占位数
  size += table_meta_pages_.size() * 8;  // table id and page id
  size += index_meta_pages_.size() * 8;  // index id and page id
  return size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  LOG(INFO)<< "into CatalogManager::CatalogManager";
  if (init) {
    CatalogMeta *meta = new CatalogMeta();
    catalog_meta_ = meta;
    FlushCatalogMetaPage();
    return;
  }
  // 将磁盘中的meta page读入内存
  page_id_t meta_page_id = CATALOG_META_PAGE_ID;
  Page *meta_page = buffer_pool_manager_->FetchPage(meta_page_id);
  char *meta_page_ptr = meta_page->GetData();
  catalog_meta_ = CatalogMeta::DeserializeFrom(meta_page_ptr);

  for(auto current_it = catalog_meta_->table_meta_pages_.begin(); current_it!= catalog_meta_->table_meta_pages_.end(); current_it++) {
    // 如果当前table的pageid是有效的，对其meta进行反序列化并初始化器TableInfo
    if (current_it->second != INVALID_PAGE_ID) {
      Page *table_meta_page = buffer_pool_manager_->FetchPage(current_it->second);
      char *table_meta_page_ptr = table_meta_page->GetData();
      TableMetadata *table_meta;
      TableMetadata::DeserializeFrom(table_meta_page_ptr, table_meta);
      TableHeap *table_heap;
      // 这里我不太理解，就是按照看到要初始化info需要heap,然后从meta中提取符合语法要求的内容填写进去搞了一个heap出来
      table_heap-TableHeap::Create(buffer_pool_manager_,table_meta->GetTableId(),table_meta->GetSchema(),log_manager_,lock_manager_);
      TableInfo *table_info;
      table_info->Init(table_meta, table_heap);
      // 获得tableinfo和tablemeta后开始建立映射关系
      tables_.emplace(table_meta->GetTableId(), table_info);
      table_names_.emplace(table_meta->GetTableName(), table_meta->GetTableId());
    }
  }
  // index的处理逻辑与table类似
  for(auto current_it = catalog_meta_->index_meta_pages_.begin(); current_it!= catalog_meta_->index_meta_pages_.end(); current_it++){
    // 如果当前index的pageid是有效的，对其meta进行反序列化并初始化器IndexInfo
    if (current_it->second != INVALID_PAGE_ID) {
      Page *index_meta_page = buffer_pool_manager_->FetchPage(current_it->second);
      char *index_meta_page_ptr = index_meta_page->GetData();
      IndexMetadata *index_meta;
      IndexMetadata::DeserializeFrom(index_meta_page_ptr, index_meta);
      IndexInfo *index_info;
      // 这里利用上面部分建立的映射提取对应的tableinfo
      index_info->Init(index_meta,tables_.at(index_meta->GetTableId()),buffer_pool_manager_);
      // 获得indexinfo和indexmeta后开始建立映射关系
      indexes_.emplace(index_meta->GetIndexId(), index_info);
      index_names_.emplace(index_meta->GetIndexName(), index_meta->GetIndexId());
    }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // 检查表是否已经存在
  LOG(INFO)<< "into CatalogManager::CreateTable";
  if (table_names_.count(table_name) > 0) {
    return DB_TABLE_ALREADY_EXIST;
  }
  // 申请一个新的page，并将table的元数据写入其中
  page_id_t table_meta_page_id;
  table_id_t table_id = next_table_id_++;
  Page *table_meta_page = nullptr;
  if ((table_meta_page = buffer_pool_manager_->NewPage(table_meta_page_id)) == nullptr) {
    return DB_FAILED;
  }
  Schema *deepcopy_schema = Schema::DeepCopySchema(schema);
  // 创建TableMetadata
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, table_meta_page_id, deepcopy_schema);
  catalog_meta_->table_meta_pages_[table_id] = table_meta_page_id;
  catalog_meta_->table_meta_pages_[next_table_id_] = -1;

  if (table_meta == nullptr) {
    return DB_FAILED;
  }
  // 序列化TableMetadata到页面

  char *table_meta_page_ptr = table_meta_page->GetData();
  table_meta->SerializeTo(table_meta_page_ptr);
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);
  // 更新catalog metadata
  catalog_meta_->table_meta_pages_.emplace(table_id, table_meta_page_id);
  // 序列化CatalogMeta到元数据页面
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (catalog_meta_page == nullptr) {
    return DB_FAILED;
  }
  char *catalog_meta_page_ptr = catalog_meta_page->GetData();
  catalog_meta_->SerializeTo(catalog_meta_page_ptr);
  buffer_pool_manager_->UnpinPage(META_PAGE_ID, true);
  // 创建TableHeap

  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, deepcopy_schema, txn,log_manager_, lock_manager_);
  if (table_heap == nullptr) {
    return DB_FAILED;
  }

  // 初始化TableInfo
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  // 更新内存中的表信息
  tables_.emplace(table_id, table_info);
  table_names_.emplace(table_name, table_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  LOG(INFO)<< "into CatalogManager::GetTable";
  std::unordered_map<std::string, table_id_t> name_to_id = table_names_;
  if (name_to_id.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = name_to_id.at(table_name);
  table_info = tables_.at(table_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  LOG(INFO)<< "into CatalogManager::GetTables";
  std::unordered_map<table_id_t, TableInfo *>::const_iterator id_to_info = tables_.begin();
  for(id_to_info;id_to_info!=tables_.end();id_to_info++)
  {
    tables.push_back(id_to_info->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  LOG(INFO)<< "into CatalogManager::CreateIndex";
  if (table_names_.count(table_name) <= 0)
    return DB_TABLE_NOT_EXIST;

  if(index_names_.count(index_name) > 0) {
    return DB_INDEX_ALREADY_EXIST;
  }
  page_id_t index_meta_page_id;
  index_id_t index_id = next_index_id_;
  next_index_id_++;
  Page *index_meta_page = nullptr;
  buffer_pool_manager_->NewPage(index_meta_page_id);
  if ((index_meta_page = buffer_pool_manager_->NewPage(index_meta_page_id))==nullptr) {
    return DB_FAILED;
  }
  table_id_t table_id = table_names_.at(table_name);
  TableSchema *table_schema = tables_.at(table_id)->GetSchema();
  std::vector<uint32_t> key_map;
  uint32_t key_index;
  // 根据初始化参数要求倒推，需要获得key_map
  for(auto key:index_keys) {
    if(table_schema->GetColumnIndex(key,key_index) != DB_COLUMN_NAME_NOT_EXIST)
      key_map.push_back(key_index);
    else
      return DB_COLUMN_NAME_NOT_EXIST;
  }
  IndexMetadata *index_meta = IndexMetadata::Create(index_id, index_name, table_id,key_map);
  if (index_meta == nullptr) {
    return DB_FAILED;
  }
  char *index_meta_page_ptr = index_meta_page->GetData();
  // 序列化index metadata
  index_meta->SerializeTo(index_meta_page_ptr);
  // 更新catalog metadata
  catalog_meta_->index_meta_pages_.emplace(index_id, index_meta_page_id);
  // 创建IndexInfo
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, tables_.at(table_id), buffer_pool_manager_);
  // 更新内存中的索引信息
  indexes_.emplace(index_id, index_info);
  index_names_.emplace(index_name, index_id);

  // 序列化CatalogMeta到元数据页面
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(META_PAGE_ID);
  if (catalog_meta_page == nullptr) {
    return DB_FAILED;
  }
  char *catalog_meta_page_ptr = catalog_meta_page->GetData();
  catalog_meta_->SerializeTo(catalog_meta_page_ptr);

  buffer_pool_manager_->UnpinPage(index_meta_page_id, true);
  buffer_pool_manager_->UnpinPage(META_PAGE_ID, true);
  return DB_SUCCESS;
}
/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  LOG(INFO)<< "into CatalogManager::GetIndex";
  if (index_names_.count(index_name) <= 0) return DB_INDEX_NOT_FOUND;
  if(table_names_.count(table_name) <= 0)
    return DB_TABLE_NOT_EXIST;
  unordered_map<string, unordered_map<string, unsigned int>>::mapped_type table_name_to_index_id = index_names_.at(index_name);
  index_info = indexes_.at(table_name_to_index_id.at(table_name));
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  LOG(INFO)<< "into CatalogManager::GetTableIndexes";
  if (table_names_.count(table_name) <= 0)
    return DB_TABLE_NOT_EXIST;
  unordered_map<string, unordered_map<string, index_id_t>>::mapped_type index_name_to_id = index_names_.at(table_name);
  for(auto index_id:index_name_to_id) {
    indexes.push_back(indexes_.at(index_id.second));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  LOG(INFO)<< "into CatalogManager::DropTable";
  if(table_names_.count(table_name) <= 0) return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_names_.at(table_name);
  // 先删除表的元数据
  page_id_t table_meta_page_id = catalog_meta_->table_meta_pages_.at(table_id);
  catalog_meta_->table_meta_pages_.erase(table_id);
  // 再删除表的索引
  for(auto index_id:index_names_.at(table_name)) {
    catalog_meta_->index_meta_pages_.erase(index_id.second);
  }
  // 最后删除表的表信息
  tables_.erase(table_id);
  // 序列化CatalogMeta到元数据页面
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(META_PAGE_ID);
  if (catalog_meta_page == nullptr) {
    return DB_FAILED;
  }
  char *catalog_meta_page_ptr = catalog_meta_page->GetData();
  catalog_meta_->SerializeTo(catalog_meta_page_ptr);
  buffer_pool_manager_->UnpinPage(META_PAGE_ID, true);
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  LOG(INFO)<< "into CatalogManager::DropIndex";
  if(table_names_.count(table_name) <= 0) return DB_TABLE_NOT_EXIST;
  if(index_names_.count(index_name) <= 0) return DB_INDEX_NOT_FOUND;
  index_id_t index_id = index_names_.at(index_name).at(table_name);
  // 先删除索引的元数据
  page_id_t index_meta_page_id = catalog_meta_->index_meta_pages_.at(index_id);
  catalog_meta_->index_meta_pages_.erase(index_id);
  // 再删除索引的表信息
  indexes_.erase(index_id);
  // 最后删除索引的名称信息
  index_names_.at(table_name).erase(index_name);
  // 序列化CatalogMeta到元数据页面
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(META_PAGE_ID);
  if (catalog_meta_page == nullptr) {
    return DB_FAILED;
  }
  char *catalog_meta_page_ptr = catalog_meta_page->GetData();
  catalog_meta_->SerializeTo(catalog_meta_page_ptr);
  buffer_pool_manager_->UnpinPage(META_PAGE_ID, true);
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  LOG(INFO)<< "into CatalogManager::FlushCatalogMetaPage";
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(META_PAGE_ID);
  if (catalog_meta_page == nullptr) {
    return DB_FAILED;
  }
  char *catalog_meta_page_ptr = catalog_meta_page->GetData();
  catalog_meta_->SerializeTo(catalog_meta_page_ptr);
  buffer_pool_manager_->UnpinPage(META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  LOG(INFO)<< "into CatalogManager::LoadTable";
  if(catalog_meta_->table_meta_pages_.count(table_id) >= 0) return DB_FAILED;
  catalog_meta_->table_meta_pages_.emplace(table_id, page_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  LOG(INFO)<< "into CatalogManager::LoadIndex";
  if(catalog_meta_->index_meta_pages_.count(index_id) >= 0) return DB_FAILED;
  catalog_meta_->index_meta_pages_.emplace(index_id, page_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  LOG(INFO)<< "into CatalogManager::GetTable";
  if (tables_.count(table_id) <= 0) return DB_TABLE_NOT_EXIST;
  table_info = tables_.at(table_id);
  return DB_SUCCESS;
}