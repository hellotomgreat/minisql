#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
//  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  LOG(INFO) << "----INTO Column::SerializeTo()----";
  uint32_t buf_offset = 0;
  uint32_t magic_number = COLUMN_MAGIC_NUM;
  MACH_WRITE_UINT32(buf, magic_number);
  buf_offset += sizeof(magic_number);

  uint32_t type_size = sizeof(TypeId);
  MACH_WRITE_TO(TypeId,buf+buf_offset, GetType());
  buf_offset += type_size;

  uint32_t len_size = sizeof(GetLength());
  MACH_WRITE_UINT32(buf+buf_offset, GetLength());
  buf_offset += len_size;

  uint32_t name_size = name_.length();
  MACH_WRITE_UINT32(buf+buf_offset, name_size);
  buf_offset += sizeof(name_size);

  // 之所以把name放在后面序列化，是为了避免反序列化的时候找不到name的长度，这里竟然能有trick
  uint32_t column_name_size = name_.length();
  MACH_WRITE_STRING(buf+buf_offset, GetName());
  buf_offset += column_name_size;

  uint32_t table_ind_size = sizeof(GetTableInd());
  MACH_WRITE_UINT32(buf+buf_offset, GetTableInd());
  buf_offset += table_ind_size;

  uint32_t nullable_size = sizeof(IsNullable());
  MACH_WRITE_TO(bool,buf+buf_offset, IsNullable());
  buf_offset += nullable_size;

  uint32_t unique_size = sizeof(IsUnique());
  MACH_WRITE_TO(bool,buf+buf_offset, IsUnique());
  buf_offset += unique_size;
  return buf_offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  LOG(INFO) << "----INTO Column::GetSerializedSize()----";
  uint32_t size = 0;
  size += sizeof(uint32_t);  // magic number
  size += sizeof(TypeId);  // type
  size += sizeof(uint32_t);  // length
  size += sizeof(uint32_t);  // name size
  size += name_.length();  // name
  size += sizeof(uint32_t);  // table index
  size += sizeof(bool);  // nullable
  size += sizeof(bool);  // unique
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  LOG(INFO) << "----INTO Column::DeserializeFrom()----";
  // replace with your code here
  uint32_t buf_offset = 0;

  uint32_t magic_number;
  uint32_t magic_number_size = sizeof(magic_number);
  magic_number = MACH_READ_UINT32(buf+buf_offset);
  buf_offset += magic_number_size;
  ASSERT(magic_number == COLUMN_MAGIC_NUM, "Wrong magic number.");

  TypeId type;
  uint32_t type_size = sizeof(type);
  type = MACH_READ_FROM(TypeId,buf+buf_offset);
  buf_offset += type_size;

  uint32_t len;
  uint32_t len_size = sizeof(len);
  len = MACH_READ_UINT32(buf+buf_offset);
  buf_offset += len_size;

  uint32_t name_size;
  uint32_t name_size_size = sizeof(name_size);
  name_size = MACH_READ_UINT32(buf+buf_offset);
  buf_offset += name_size_size;

  char *name = new char[name_size];
  memcpy(name,buf+buf_offset, name_size);
  buf_offset += name_size;

  uint32_t table_ind;
  uint32_t table_ind_size = sizeof(table_ind);
  table_ind = MACH_READ_UINT32(buf+buf_offset);
  buf_offset += table_ind_size;

  bool nullable;
  uint32_t nullable_size = sizeof(nullable);
  nullable = MACH_READ_FROM(bool,buf+buf_offset);
  buf_offset += nullable_size;

  bool unique;
  uint32_t unique_size = sizeof(unique);
  unique = MACH_READ_FROM(bool,buf+buf_offset);
  buf_offset += unique_size;

  column = new Column(name, type, len, table_ind, nullable, unique);
  return buf_offset;
}