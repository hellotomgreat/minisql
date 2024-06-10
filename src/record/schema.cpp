#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  LOG(INFO) << "---Into Schema::SerializeTo() ---";
  // 序列化魔数
  uint32_t magic_number = this->SCHEMA_MAGIC_NUM;
  MACH_WRITE_UINT32(buf, magic_number);
  uint32_t buf_offset = sizeof(magic_number);
  // 序列化列数
  uint32_t column_num = GetColumnCount();
  MACH_WRITE_UINT32(buf + buf_offset, column_num);
  buf_offset += sizeof(column_num);
  // 序列化列
  for(auto column: this->GetColumns()) {
    // 直接调用column的序列化方法
    column->SerializeTo(buf + buf_offset);
    buf_offset += column->GetSerializedSize();
  }
  // 序列化是否管理
  bool is_manage = this->is_manage_;
  MACH_WRITE_TO(bool, buf + buf_offset, is_manage);
  buf_offset += sizeof(bool);
  return buf_offset;
}

uint32_t Schema::GetSerializedSize() const {
  LOG(INFO) << "---Into Schema::GetSerializedSize() ---";
  uint32_t size = 0;
  size += sizeof(uint32_t); // 序列化魔数大小
  size += sizeof(uint32_t); // 序列化列数大小
  for(auto column: this->GetColumns()) {
    uint32_t column_size = column->GetSerializedSize();
    size += column_size;
  }
  size += sizeof(bool);
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // 反序列化魔数
  LOG(INFO) << "---Into Schema::DeserializeFrom() ---";
  uint32_t buf_offset = 0;
  uint32_t magic_number = MACH_READ_FROM(uint32_t, buf+buf_offset);
  ASSERT(magic_number == SCHEMA_MAGIC_NUM, "Invalid magic number");
  buf_offset += sizeof(magic_number);
  // 反序列化列数
  uint32_t column_num = MACH_READ_FROM(uint32_t, buf+buf_offset);
  buf_offset += sizeof(column_num);
  // 反序列化列
  std::vector<Column*> columns;
  for(uint32_t i = 0; i < column_num; i++) {
    Column *column;
    uint32_t column_offset = Column::DeserializeFrom(buf+buf_offset,column);
    columns.push_back(column);
    buf_offset += column_offset;
  }
  // 反序列化是否管理
  bool is_manage = MACH_READ_FROM(bool, buf+buf_offset);
  buf_offset += sizeof(bool);
  schema = new Schema(columns, is_manage);
  return buf_offset;
}