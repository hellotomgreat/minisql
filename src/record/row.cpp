#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  LOG(INFO)<< "---Into Row::SerializeTo()---";
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t column_size = schema->GetColumnCount();
  uint32_t buf_offset = 0;
  RowId row_id = GetRowId();
  MACH_WRITE_TO(RowId, buf+buf_offset, row_id);
  buf_offset += sizeof(row_id);

  Field *f;
  for(uint32_t i = 0; i < column_size; i++) {
    f = fields_[i];
    f->SerializeTo(buf);
    buf_offset += f->GetSerializedSize();
  }

  return buf_offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  LOG(INFO)<< "---Into Row::DeserializeFrom()---";
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t column_size = schema->GetColumnCount();
  uint32_t buf_offset = 0;
  RowId row_id;

  MACH_READ_FROM(RowId, buf+buf_offset, row_id);
  buf_offset += sizeof(row_id);

  for(int i=0;i<int(column_size);i++){
    Field *f;
    buf_offset+=f->Field::DeserializeFrom(buf_offset + buf, schema->GetColumn(i)->GetType(), &f, false);
    fields_.push_back(f);
  }
  return buf_offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  LOG(INFO)<< "---Into Row::GetSerializedSize()---";
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = 0;
  size += sizeof(RowId);

  uint32_t column_size = schema->GetColumnCount();
  for(uint32_t i = 0; i < column_size; i++) {
    size += fields_[i]->GetSerializedSize();
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  LOG(INFO)<< "---Into Row::GetKeyFromRow()---";
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
